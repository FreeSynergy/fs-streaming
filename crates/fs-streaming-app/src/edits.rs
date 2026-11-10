//! Reading an `Edit` (the command `change`): checked against the description, then looked at by field.
//!
//! Pattern: Adapter — `Edit` of `fs-types` (field path, basic type, values; missing = unchanged, no values =
//! cleared) to what the core's methods take. Fields the core cannot change (state, note, address, `hasKey` ...) are
//! refused as findings before anything is applied, so a refused edit changes nothing.

use fs_admin_frontend::BackendError;
use fs_types::{BasicRules, Edit, FieldEdit, RecordIssue, RecordProblem, TypeDescription};

/// The edit with canonical values, and only fields from `changeable`; else the findings.
pub fn checked(
    edit: &Edit,
    description: &TypeDescription,
    changeable: &[&str],
) -> Result<Edit, BackendError> {
    let mut edit = edit
        .check(description, &BasicRules::standard())
        .map_err(BackendError::Rejected)?;
    let refused: Vec<RecordIssue> = edit
        .fields
        .iter()
        .filter(|f| !changeable.contains(&f.path[0].as_str()))
        .map(|f| RecordIssue {
            field: f.path[0].clone(),
            index: 0,
            problem: RecordProblem::UnknownField,
        })
        .collect();
    if refused.is_empty() {
        edit.fields
            .retain(|f| changeable.contains(&f.path[0].as_str()));
        Ok(edit)
    } else {
        Err(BackendError::Rejected(refused))
    }
}

/// The part of the edit for `field`, if it is in it.
pub fn part<'a>(edit: &'a Edit, field: &str) -> Option<&'a FieldEdit> {
    edit.fields.iter().find(|f| f.path[0] == field)
}

/// A finding that `field` is needed (a required field may not be cleared).
pub fn needed(field: &str) -> BackendError {
    BackendError::Rejected(vec![RecordIssue {
        field: field.to_owned(),
        index: 0,
        problem: RecordProblem::Missing,
    }])
}
