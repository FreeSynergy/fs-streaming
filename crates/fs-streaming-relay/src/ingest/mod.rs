//! Ingest: accepting senders over plain RTMP.
//!
//! Pattern: Facade (declarations) — a listener task and one task per connection. Ingest is plain
//! RTMP: the key travels readable, so a host that listens beyond the machine needs its own TLS.

mod conn;
mod listener;
mod path;
mod session;

pub(crate) use listener::accept_loop;
