//! The stored rows.
//!
//! Pattern: Adapter between our types and JSON — a target row holds its key as plain text, because it
//! has to be sent. That is why `Debug` here prints `[hidden]` for the key and the hash.

use std::fmt;

use fs_store::ObjectId;
use serde::{Deserialize, Serialize};

use crate::secret::Secret;

/// One stream with its targets: the content of one stored object.
#[derive(Clone, Serialize, Deserialize)]
pub struct StreamRow {
    /// Id.
    pub id: ObjectId,
    /// Name.
    pub name: String,
    /// `sha256:<hex>` of the publisher key.
    pub key_hash: String,
    /// Records when live.
    pub recording: bool,
    /// Its targets, in the order they were added.
    pub targets: Vec<TargetRow>,
}

/// One target. The key is only here, in the private store.
#[derive(Clone, Serialize, Deserialize)]
pub struct TargetRow {
    /// Id.
    pub id: ObjectId,
    /// Name.
    pub name: String,
    /// Address, never with a key.
    pub url: String,
    /// The key as given, if any.
    pub key: Option<String>,
    /// Switched on.
    pub enabled: bool,
}

impl TargetRow {
    /// The key as a secret.
    pub fn secret(&self) -> Option<Secret> {
        self.key.clone().map(Secret::new)
    }
}

impl fmt::Debug for TargetRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetRow")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("url", &self.url)
            .field("key", &self.key.as_ref().map(|_| "[hidden]"))
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl fmt::Debug for StreamRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamRow")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("key_hash", &"[hidden]")
            .field("recording", &self.recording)
            .field("targets", &self.targets)
            .finish()
    }
}
