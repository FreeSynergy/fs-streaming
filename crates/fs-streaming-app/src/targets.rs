//! The backend of `Target` over the core.
//!
//! Pattern: Adapter (to the frontend's `Backend`) + Command (one arm per command of the table). A target key is a
//! secret: `add` sends none, and `setKey` is not run (its row says why). Records never hold a key.

use fs_admin_frontend::{Backend, BackendError, Outcome, Record, Values};
use fs_streaming_core::Target;
use fs_streaming_core::describe::Owner;
use fs_types::{DescribesType, Edit};

use crate::core_link::CoreLink;
use crate::edits::{checked, needed, part};
use crate::errors::{backend_error, unsupported};
use crate::ids::from_iri;
use crate::ids::to_iri;
use crate::records;
use crate::streams::row_supported;
use crate::values::{flag, flag_of, id_of, matches, one, record_of};

/// The targets of all streams.
pub struct TargetBackend {
    core: CoreLink,
}

impl TargetBackend {
    /// Targets over `core`.
    pub fn new(core: CoreLink) -> Self {
        Self { core }
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
            Some(text) => Some(from_iri(text).ok_or(BackendError::NotFound)?),
            None => None,
        };
        let term = one(query, "term").unwrap_or_default();
        Ok(self
            .all(only)?
            .iter()
            .filter(|t| matches(term, &t.name))
            .map(records::target)
            .collect())
    }

    fn supported(&self, command: &str) -> bool {
        row_supported(Owner::Target, command) == Some(true)
    }

    fn change(&self, record: &str, edit: &Edit) -> Result<Outcome, BackendError> {
        let id = from_iri(record).ok_or(BackendError::NotFound)?;
        let edit = checked(edit, &Target::describe(), &["name", "url", "enabled"])?;
        let streams = self.core.streams();
        let enabled = part(&edit, "enabled")
            .map(|p| {
                p.values
                    .first()
                    .and_then(|v| flag_of(v))
                    .ok_or_else(|| needed("enabled"))
            })
            .transpose()?;
        let text = |field: &str| {
            part(&edit, field)
                .map(|p| p.values.first().cloned().ok_or_else(|| needed(field)))
                .transpose()
        };
        let (name, url) = (text("name")?, text("url")?);
        let current = self
            .all(None)?
            .into_iter()
            .find(|t| t.id == id)
            .ok_or(BackendError::NotFound)?;
        if name.is_some() || url.is_some() {
            // The core changes name and address together: what the edit leaves out stays as it is.
            self.core
                .wait(streams.edit_target(
                    id,
                    name.as_deref().unwrap_or(&current.name),
                    url.as_deref().unwrap_or(&current.url),
                ))
                .map_err(|e| backend_error(&e))?;
        }
        if let Some(on) = enabled {
            self.core
                .wait(streams.set_target_enabled(id, on))
                .map_err(|e| backend_error(&e))?;
        }
        Ok(Outcome {
            id: Some(to_iri(id)),
            values: Values::new(),
        })
    }

    fn run(
        &self,
        name: &str,
        record: Option<&str>,
        input: &Values,
    ) -> Result<Outcome, BackendError> {
        match row_supported(Owner::Target, name) {
            None => return Err(BackendError::NotFound),
            Some(false) => return Err(unsupported(name)),
            Some(true) => {}
        }
        let streams = self.core.streams();
        let missing = || BackendError::Rejected(Vec::new());
        let id = record_of(record, input);
        let done = |id| Outcome {
            id: Some(to_iri(id)),
            values: Values::new(),
        };
        match name {
            "find" => Ok(Outcome {
                id: None,
                values: Values::from([(
                    "items".into(),
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
            // `change` is a method of `Backend`, with an `Edit`; it never arrives here.
            "change" => Err(BackendError::Unavailable(
                "change goes through Backend::change".into(),
            )),
            _ => Err(BackendError::Unavailable(format!("no handler for {name}"))),
        }
    }
}
