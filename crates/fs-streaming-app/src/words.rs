//! The words of the program: one English table, looked up by key.
//!
//! Pattern: Strategy (`Say` of `fs-components-fields`) — the frontend asks for keys, this table answers in
//! English; a translation (ADR 0053) replaces the table, not its users. A missing key comes back as the key.
//! Keys: those the generated frontend asks for, then the labels of the two descriptions (`words_labels`).

use fs_components_fields::Say;

use crate::words_labels::LABELS;

/// What the generated frontend asks for itself.
const FRONTEND: &[(&str, &str)] = &[
    ("menu.program", "Program"),
    ("menu.view", "View"),
    ("menu.help", "Help"),
    ("menu.quit", "Quit"),
    ("menu.about", "About"),
    ("action.add", "Add"),
    ("action.back", "Back"),
    ("action.cancel", "Cancel"),
    ("action.close", "Close"),
    ("action.remove", "Remove"),
    ("action.save", "Save"),
    ("command.soon", "Coming soon"),
    ("confirm.sure", "Are you sure?"),
    ("detail.not-loaded", "This record is not loaded."),
    ("finding.slot", "Entry {n}"),
    ("list.empty", "Nothing here yet."),
    ("list.separator", ", "),
    ("list.stale", "The list may be out of date."),
    ("value.none", "none"),
    ("sort.ascending", "ascending"),
    ("sort.descending", "descending"),
    ("about.version", "Version"),
    ("about.origin", "Origin"),
    ("about.licence", "Licence"),
    ("about.repository", "Repository"),
    ("about.unoffered", "Not offered here"),
    ("message.done", "Done."),
    ("message.rejected", "The program refused this."),
    ("message.not-found", "It is not there any more."),
    ("message.unavailable", "The program cannot do this now."),
    ("message.unsaved", "There is text that was not sent yet."),
    ("problem.missing", "This is needed."),
    ("problem.unknown-field", "This field does not exist."),
    ("problem.not-multiple", "Only one value is allowed."),
    ("problem.empty", "Nothing typed."),
    ("problem.too-long", "Too long."),
    ("problem.character", "A character that is not allowed."),
    ("problem.uri.bad", "This is not a valid address."),
    (
        "problem.uri.not-absolute",
        "The address needs a scheme, like rtmp://.",
    ),
    (
        "problem.uri.unsafe-scheme",
        "This kind of address is not allowed.",
    ),
    ("problem.date-time.bad", "This is not a date and time."),
    ("problem.choice.not-in-list", "Pick one of the choices."),
    ("hint.stream.name", "A name for the stream"),
    ("hint.stream.rename.name", "A new name"),
    ("hint.stream.create.name", "A name for the stream"),
    ("hint.target.name", "A name for the target"),
    (
        "hint.target.url",
        "rtmp://host/application or rtmps://host/application",
    ),
    ("hint.target.add.name", "A name for the target"),
    (
        "hint.target.add.url",
        "rtmp://host/application or rtmps://host/application",
    ),
    ("hint.target.edit.name", "A name for the target"),
    (
        "hint.target.edit.url",
        "rtmp://host/application or rtmps://host/application",
    ),
];

/// The title of the window.
pub const TITLE: &str = "FreeSynergy Streaming";

/// The English words.
#[derive(Clone, Copy, Debug, Default)]
pub struct Words;

impl Words {
    /// The English text of `key`, if the table has it.
    pub fn english(key: &str) -> Option<&'static str> {
        FRONTEND
            .iter()
            .chain(LABELS.iter())
            .find(|(k, _)| *k == key)
            .map(|(_, v)| *v)
    }
}

impl Say for Words {
    fn say(&self, key: &str) -> String {
        Self::english(key).unwrap_or(key).to_owned()
    }
}
