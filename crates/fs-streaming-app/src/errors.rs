//! The core's errors as the frontend's.
//!
//! Pattern: Adapter — a refusal of the core becomes `Rejected` (with a finding on the address when that is what
//! was wrong), a missing thing `NotFound`, anything else `Unavailable` with a text for the log. The core's error
//! texts are fixed wording and hold no key (core `error.rs`).

use fs_admin_frontend::BackendError;
use fs_streaming_core::Error;
use fs_types::{RecordIssue, RecordProblem, TypeProblem};

/// The frontend's form of `error`.
pub fn backend_error(error: &Error) -> BackendError {
    match error {
        Error::NotFound(_) => BackendError::NotFound,
        Error::InvalidUrl(_) => BackendError::Rejected(vec![RecordIssue {
            field: "url".into(),
            index: 0,
            problem: RecordProblem::Value(TypeProblem::BadUri),
        }]),
        Error::Refused(_) | Error::InvalidInput(_) => BackendError::Rejected(Vec::new()),
        Error::Unsupported(_) | Error::Store(_) | Error::Corrupt | Error::NoRelay => {
            BackendError::Unavailable(error.to_string())
        }
    }
}

/// The answer for a command the app does not run: the name after `unsupported:`.
pub fn unsupported(command: &str) -> BackendError {
    BackendError::Unavailable(format!("unsupported:{command}"))
}
