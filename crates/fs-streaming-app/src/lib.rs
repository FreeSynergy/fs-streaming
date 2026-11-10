//! `fs-streaming-app`: the window of FreeSynergy.Streaming (ADR 0065, 0066).
//!
//! No drawing code and no look here: the window is the standard shell around the generated admin frontend
//! (`fs-admin-frontend`), which is told two type descriptions (`Stream`, `Target`) and gets two backends over the
//! core. See `DESIGN.md`.
//!
//! Pattern: Facade — this file only declares modules and re-exports the public API.

#![deny(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::module_name_repetitions
)]

mod about;
mod args;
mod core_link;
mod errors;
mod ids;
mod open;
mod records;
mod streams;
mod targets;
mod values;
mod window;
mod words;
mod words_labels;

pub use about::about;
pub use args::{Args, HELP, parse};
pub use core_link::CoreLink;
pub use open::{Program, open_program};
pub use streams::StreamBackend;
pub use targets::TargetBackend;
pub use window::{app_command, program_window};
pub use words::Words;
