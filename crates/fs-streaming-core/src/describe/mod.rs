//! The type description of the two data types (ADR 0066): what a generated frontend is told.
//!
//! Pattern: Registry entry (trait first) — `Stream` and `Target` implement `DescribesType` of `fs-types`.
//! The commands come from ONE table ([`table`]) that lists every capability id of `requirements.md`.

mod fields;
mod planned_rows;
mod row;
mod stream;
mod stream_rows;
mod table;
mod target;
mod target_rows;

pub use row::{Owner, Row};
pub use table::{commands_of, table};
