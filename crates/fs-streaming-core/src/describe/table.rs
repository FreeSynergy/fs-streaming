//! The command table and the one switch.
//!
//! Pattern: Registry — one table of every command of both types, supported or not, and ONE function
//! ([`commands_of`]) that turns rows into the commands a description reports. Until `fs-types` has the field
//! `supported` (Thread T, `types.capnp`), the description carries all commands and no flag, and the app answers
//! `unsupported:<name>` for the rows with `supported == false` (the Backend, not the frontend). When the field
//! exists, set it in `to_command` below: that is the only change in this crate.

use fs_types::CommandDescription;

use super::row::{Owner, Row};
use super::{planned_rows, stream_rows, target_rows};

/// Every command of both types, in the order a frontend lists them. Each capability id of `requirements.md` is in
/// the `capabilities` of exactly one row; a test fails when one is missing.
pub fn table() -> Vec<Row> {
    let mut rows = stream_rows::rows();
    rows.extend(planned_rows::rows());
    rows.extend(target_rows::rows());
    rows
}

/// THE SWITCH: one row to the command the description reports.
///
/// Today every row becomes a plain command. With the field `supported` in `fs-types` this is where
/// `row.supported` is written into it (and `row.reason` as its reason, if the field has one).
fn to_command(row: &Row) -> CommandDescription {
    row.command.clone()
}

/// The commands the description of `owner` reports: all rows of the owner, supported or not.
pub fn commands_of(owner: Owner) -> Vec<CommandDescription> {
    table()
        .iter()
        .filter(|r| r.owner == owner)
        .map(to_command)
        .collect()
}
