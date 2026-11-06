//! Notes: the fixed wording shown to people when something went wrong.
//!
//! Pattern: Registry (enum of known sentences) — a note is chosen from this list, never copied from
//! what the far end said, so a key echoed by a hostile server cannot reach a note.

use std::fmt;

/// One sentence of our own. Never contains text from the far end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Note {
    /// TCP connection failed.
    Unreachable,
    /// TLS failed or the certificate did not fit the host name.
    TlsFailed,
    /// No answer within the time allowed.
    NoAnswer,
    /// The far end closed the connection.
    ConnectionClosed,
    /// The far end refused the connection to its application.
    ConnectRefused,
    /// The far end explicitly refused the key or the stream.
    PublishRefused,
    /// The far end answered in a way that could not be followed.
    ProtocolError,
    /// The key went over plain rtmp.
    KeyTravelsReadable,
    /// No key set, only the connection was tried.
    NoKeyToTry,
    /// The far end said yes; it may still refuse later.
    KeyAccepted,
    /// The check was cancelled.
    Cancelled,
    /// The recording could not be written.
    RecordingFailed,
    /// The recording could not keep up.
    RecordingTooSlow,
    /// The relay could not listen on its address.
    CannotListen,
}

impl Note {
    /// The sentence.
    pub const fn text(self) -> &'static str {
        match self {
            Self::Unreachable => "The far end could not be reached.",
            Self::TlsFailed => {
                "The secure connection failed: the certificate or the host name did not fit."
            }
            Self::NoAnswer => "The far end did not answer in time.",
            Self::ConnectionClosed => "The far end closed the connection.",
            Self::ConnectRefused => "The far end refused the connection to its application.",
            Self::PublishRefused => "The far end refused the key or the stream.",
            Self::ProtocolError => "The far end answered in a way that could not be followed.",
            Self::KeyTravelsReadable => {
                "The key was sent over plain rtmp, so it travelled readable; use rtmps if the far end offers it."
            }
            Self::NoKeyToTry => "No key is set, so only the connection was tried.",
            Self::KeyAccepted => "The far end accepted the key; a server may still refuse later.",
            Self::Cancelled => "The check was cancelled.",
            Self::RecordingFailed => "The recording could not be written; the stream goes on.",
            Self::CannotListen => {
                "The relay could not listen on its address, so nothing can be sent to it."
            }
            Self::RecordingTooSlow => {
                "The recording could not keep up and was stopped; the stream goes on."
            }
        }
    }
}

impl fmt::Display for Note {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text())
    }
}
