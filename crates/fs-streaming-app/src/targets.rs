//! The backend of `Target` over the core.
//!
//! Pattern: Adapter (to the frontend's `Backend`) + Command (one arm per command of the table). A target key is a
//! secret: `add` sends none, and `setKey` is not run (its row says why). Records never hold a key.

use fs_admin_frontend::{Backend, BackendError, Outcome, Record, Values};
use fs_streaming_core::Target;
use fs_streaming_core::describe::Owner;

use crate::core_link::CoreLink;
use crate::errors::{backend_error, unsupported};
use crate::ids::to_iri;
use crate::records;
use crate::streams::row_supported;
use crate::values::{flag, id_of, one};

/// The targets of all streams.
pub struct TargetBackend {
    core: CoreLink,
}

impl TargetBackend {
    /// Targets over `core`.
    pub fn new(core: CoreLink) -> Self {
        Self { core }
    }

    /// Whether the core can carry out `command`; see `StreamBackend::supported`.
    pub fn supported(&self, command: &str) -> bool {
        row_supported(Owner::Target, command) == Some(true)
    }

    fn all(&self, only: Option<fs_objects::ObjectId>) -> Result<Vec<Target>, BackendError> {
        let streams = self.core.streams();
        let ids = match only {
            Some(id) => vec![id],
            None => self
                .core
                .all_streams()
                .map_err(|e| backend_error(&e))?
                .iter()
                .map(|s| s.id)
                .collect(),
        };
        let mut out = Vec::new();
        for id in ids {
            out.extend(
                self.core
                    .wait(streams.targets(id))
                    .map_err(|e| backend_error(&e))?,
            );
        }
        Ok(out)
    }
}

impl Backend for TargetBackend {
    fn list(&self, query: &Values) -> Result<Vec<Record>, BackendError> {
        let only = match one(query, "stream") {
            Some(text) => Some(crate::ids::from_iri(text).ok_or(BackendError::NotFound)?),
            None => None,
        };
        Ok(self.all(only)?.iter().map(records::target).collect())
    }

    fn run(&self, name: &str, input: &Values) -> Result<Outcome, BackendError> {
        match row_supported(Owner::Target, name) {
            None => return Err(BackendError::NotFound),
            Some(false) => return Err(unsupported(name)),
            Some(true) => {}
        }
        let streams = self.core.streams();
        let missing = || BackendError::Rejected(Vec::new());
        let id = id_of(input, "id");
        let done = |id| Outcome {
            id: Some(to_iri(id)),
            values: Values::new(),
        };
        match name {
            "find" => Ok(Outcome {
                id: None,
                values: Values::from([(
                    "targets".into(),
                    self.list(input)?.into_iter().map(|r| r.id).collect(),
                )]),
            }),
            "add" => {
                let stream = id_of(input, "stream").ok_or_else(missing)?;
                let (n, u) = (one(input, "name"), one(input, "url"));
                // No key: a key is a secret and has no input here.
                let t = self
                    .core
                    .wait(streams.add_target(
                        stream,
                        n.ok_or_else(missing)?,
                        u.ok_or_else(missing)?,
                        "",
                    ))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(t.id))
            }
            "edit" => {
                let (n, u) = (one(input, "name"), one(input, "url"));
                let t = self
                    .core
                    .wait(streams.edit_target(
                        id.ok_or_else(missing)?,
                        n.ok_or_else(missing)?,
                        u.ok_or_else(missing)?,
                    ))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(t.id))
            }
            "setEnabled" => {
                let on = flag(input, "enabled").ok_or_else(missing)?;
                let t = self
                    .core
                    .wait(streams.set_target_enabled(id.ok_or_else(missing)?, on))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(t.id))
            }
            "remove" => {
                let id = id.ok_or_else(missing)?;
                self.core
                    .wait(streams.remove_target(id))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(id))
            }
            _ => Err(BackendError::Unavailable(format!("no handler for {name}"))),
        }
    }
}
