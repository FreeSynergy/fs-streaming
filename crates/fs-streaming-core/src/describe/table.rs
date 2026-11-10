//! The command table and the one switch.
//!
//! Pattern: Registry — one table of every command of both types, supported or not, and ONE function
//! ([`to_command`], marked `// SWITCH`) that turns a row into the command the description reports, with the
//! flag `supported` of `fs-types` set from `row.supported`. The table stays as the source of the reasons (for the
//! developer, in DESIGN.md and tests) and of the capability ids that a test checks against `requirements.md`.

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

/// SWITCH: one row to the command the description reports. `supported` is the field of `CommandDescription`
/// (`types.capnp`, `fs-types` v0.0.11): a row the core cannot do is reported `unsupported()`, "coming soon" in a
/// frontend, which frees it when this flips. Flip `supported` in the row, nowhere else.
fn to_command(row: &Row) -> CommandDescription {
    // SWITCH
    let c = row.command.clone();
    if row.supported { c } else { c.unsupported() }
}

/// The commands the description of `owner` reports: all rows of the owner, supported or not.
pub fn commands_of(owner: Owner) -> Vec<CommandDescription> {
    table()
        .iter()
        .filter(|r| r.owner == owner)
        .map(to_command)
        .collect()
}
