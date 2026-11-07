//! Error type of the streaming core.
//!
//! Pattern: none (plain error enum via `thiserror`) — one `Error` for every public operation. The
//! wording is fixed and our own: no key and no far-end text can be inside an error.

use crate::state::Refusal;
use crate::url::UrlProblem;

/// Result of every fallible operation of this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong in the streaming core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The object store failed.
    #[error("object store: {0}")]
    Store(#[from] fs_objects::Error),
    /// Stored JSON could not be read or written (no content is quoted).
    #[error("the stored data could not be read or written")]
    Corrupt,
    /// No stream, target or recording with this id.
    #[error("{0} not found")]
    NotFound(&'static str),
    /// The state of the stream or target does not allow it.
    #[error("refused: {0}")]
    Refused(Refusal),
    /// The target address is not acceptable.
    #[error("address refused: {0}")]
    InvalidUrl(UrlProblem),
    /// A value is not acceptable.
    #[error("refused: {0}")]
    InvalidInput(&'static str),
    /// No relay is attached yet, so nothing can run.
    #[error("no relay is attached")]
    NoRelay,
    /// The explicit "not possible" of a provider (ADR 0045).
    #[error(transparent)]
    Unsupported(#[from] fs_interfaces::Unsupported),
}

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::Corrupt
    }
}
