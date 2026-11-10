//! Record ids: a stream or target id as an AS2-style `id` (an IRI), `urn:uuid:<uuid>`.
//!
//! Pattern: Adapter — the core's `ObjectId` to the frontend's id text and back; nothing else knows the form.

use fs_objects::ObjectId;

const PREFIX: &str = "urn:uuid:";

/// The id as the frontend sees it.
pub fn to_iri(id: ObjectId) -> String {
    format!("{PREFIX}{id}")
}

/// The id from what the frontend sent back.
pub fn from_iri(text: &str) -> Option<ObjectId> {
    text.trim().strip_prefix(PREFIX)?.parse().ok()
}
