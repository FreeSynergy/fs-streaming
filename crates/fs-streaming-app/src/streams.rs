//! The backend of `Stream` over the core.
//!
//! Pattern: Adapter (to the frontend's `Backend`) + Command (one arm per command of the table). Which commands
//! may run is asked of the table of the description, not decided here; a row that is not supported answers
//! `unsupported:<name>`. The publisher key is never read, shown or kept: `create` and `rotateKey` are not run.

use fs_admin_frontend::{Backend, BackendError, Outcome, Record, Values};
use fs_streaming_core::KeyCheck;
use fs_streaming_core::describe::{Owner, table};

use crate::core_link::CoreLink;
use crate::errors::{backend_error, unsupported};
use crate::ids::to_iri;
use crate::records::{self, stream_state_key};
use crate::values::{flag, id_of, single, yes_no};

/// Whether the table has the command on `owner` and the core can do it.
pub(crate) fn row_supported(owner: Owner, name: &str) -> Option<bool> {
    table()
        .into_iter()
        .find(|r| r.owner == owner && r.command.name == name)
        .map(|r| r.supported)
}

/// The streams.
pub struct StreamBackend {
    core: CoreLink,
}

impl StreamBackend {
    /// Streams over `core`.
    pub fn new(core: CoreLink) -> Self {
        Self { core }
    }

    /// Whether the core can carry out `command`. The frontend of the rev in use does not ask yet; when its
    /// `Backend` has `supported`, this body moves into the trait impl unchanged.
    pub fn supported(&self, command: &str) -> bool {
        row_supported(Owner::Stream, command) == Some(true)
    }
}

impl Backend for StreamBackend {
    fn list(&self, _query: &Values) -> Result<Vec<Record>, BackendError> {
        let all = self.core.all_streams().map_err(|e| backend_error(&e))?;
        Ok(all.iter().map(records::stream).collect())
    }

    fn run(&self, name: &str, input: &Values) -> Result<Outcome, BackendError> {
        match row_supported(Owner::Stream, name) {
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
                    "streams".into(),
                    self.list(&Values::new())?
                        .into_iter()
                        .map(|r| r.id)
                        .collect(),
                )]),
            }),
            "rename" => {
                let (id, new) = (
                    id.ok_or_else(missing)?,
                    input.get("name").and_then(|v| v.first()),
                );
                let s = self
                    .core
                    .wait(streams.rename(id, new.ok_or_else(missing)?))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(s.id))
            }
            "delete" => {
                let id = id.ok_or_else(missing)?;
                self.core
                    .wait(streams.delete(id))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(id))
            }
            "stop" => {
                let s = self
                    .core
                    .wait(streams.stop(id.ok_or_else(missing)?))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(s.id))
            }
            "setRecording" => {
                let on = flag(input, "recording").ok_or_else(missing)?;
                let s = self
                    .core
                    .wait(streams.set_recording(id.ok_or_else(missing)?, on))
                    .map_err(|e| backend_error(&e))?;
                Ok(done(s.id))
            }
            "check" => self.check(id.ok_or_else(missing)?),
            "status" => self.status(id.ok_or_else(missing)?),
            // A row the table calls supported but this match does not know is a defect, and says so.
            _ => Err(BackendError::Unavailable(format!("no handler for {name}"))),
        }
    }
}

impl StreamBackend {
    fn check(&self, id: fs_objects::ObjectId) -> Result<Outcome, BackendError> {
        let results = self
            .core
            .wait(self.core.streams().check(id))
            .map_err(|e| backend_error(&e))?;
        let mut v = Values::new();
        for r in &results {
            let key = match r.key {
                KeyCheck::Unknown => "unknown",
                KeyCheck::Accepted => "accepted",
                KeyCheck::Rejected => "rejected",
            };
            v.entry("target".into()).or_default().push(to_iri(r.target));
            v.entry("reached".into())
                .or_default()
                .push(yes_no(r.reached).into());
            v.entry("keyCheck".into()).or_default().push(key.into());
            v.entry("note".into()).or_default().push(r.note_text());
        }
        Ok(Outcome {
            id: Some(to_iri(id)),
            values: v,
        })
    }

    fn status(&self, id: fs_objects::ObjectId) -> Result<Outcome, BackendError> {
        let s = self
            .core
            .wait(self.core.streams().status(id))
            .map_err(|e| backend_error(&e))?;
        let mut v = Values::new();
        v.insert("state".into(), single(stream_state_key(s.state)));
        v.insert("since".into(), single(s.since.to_rfc3339()));
        v.insert("bytesIn".into(), single(s.bytes_in.to_string()));
        v.insert("hasVideo".into(), single(yes_no(s.has_video)));
        v.insert("hasAudio".into(), single(yes_no(s.has_audio)));
        if let Some(n) = s.recording_problem {
            v.insert("recordingProblem".into(), single(n.text()));
        }
        Ok(Outcome {
            id: Some(to_iri(id)),
            values: v,
        })
    }
}
