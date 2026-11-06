//! The private store of streams and targets.
//!
//! Pattern: Repository over `UnloggedObjects` — rows are written without the activity log, in a store
//! the host keeps apart and never connects to the hub: the rows hold a key hash and, for targets, the
//! key itself.

mod rows;
mod store;

pub use rows::{StreamRow, TargetRow};
pub use store::Repo;
