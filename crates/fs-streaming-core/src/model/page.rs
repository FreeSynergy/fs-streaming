//! `Page` and `Cursor`: paging through streams.
//!
//! Pattern: Iterator (by cursor) — the cursor is opaque text; a caller hands it back for the next page.

/// Opaque position in a list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor(pub String);

/// One page of a list.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    /// The items.
    pub items: Vec<T>,
    /// Where the next page starts; none at the end.
    pub next: Option<Cursor>,
}
