//! The About page (ADR 0065 point 2).
//!
//! Pattern: Value object — name, version, licence and repository come from the package itself; no secret and no
//! stream is named. Where the program came from is not known to it yet (the store passes nothing), so it says
//! `source`.

use fs_admin_frontend::view::AboutInfo;

/// The name of the program, as the catalog and the window title know it.
pub const NAME: &str = "FreeSynergy.Streaming";

/// The page.
pub fn about() -> AboutInfo {
    AboutInfo {
        name: NAME.into(),
        version: format!("v{}", env!("CARGO_PKG_VERSION")),
        origin: "source".into(),
        licence: env!("CARGO_PKG_LICENSE").into(),
        repository: env!("CARGO_PKG_REPOSITORY").into(),
    }
}

/// The help page of the program.
pub const HELP_URL: &str = "https://github.com/FreeSynergy/fs-documentation/blob/main/docs/en/programs/apps/streaming/design.md";
