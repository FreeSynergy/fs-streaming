//! `check`: try targets without sending media.
//!
//! Pattern: Command — per target: connect, TLS, handshake, `connect`, and with a key a `publish` that
//! is ended at once, before any media. `accepted` only on an explicit yes, `rejected` only on an
//! explicit no, otherwise `unknown`. All targets run at once; a cancel ends them all.

use std::sync::Arc;

use fs_streaming_core::{CancelToken, CheckResult, KeyCheck, Note, TargetSpec};
use tokio::task::JoinSet;

use super::Shared;
use crate::wire::{Client, DialFailure, Dialer, PublishOutcome};

/// Run the checks; a cancel (a sender connecting, `stop`) ends the unfinished ones.
pub async fn run_checks(
    shared: &Arc<Shared>,
    specs: Vec<TargetSpec>,
    token: CancelToken,
) -> Vec<CheckResult> {
    let mut set = JoinSet::new();
    for (i, spec) in specs.iter().cloned().enumerate() {
        let dialer = shared.dialer.clone();
        set.spawn(async move { (i, check_one(&dialer, &spec).await) });
    }
    let mut results: Vec<Option<CheckResult>> = vec![None; specs.len()];
    loop {
        tokio::select! {
            joined = set.join_next() => match joined {
                Some(Ok((i, r))) => results[i] = Some(r),
                Some(Err(_)) => {}
                None => break,
            },
            () = token.cancelled() => {
                set.abort_all();
                while set.join_next().await.is_some() {}
                break;
            }
        }
    }
    specs
        .iter()
        .zip(results)
        .map(|(s, r)| {
            r.unwrap_or(CheckResult {
                target: s.id,
                reached: false,
                key: KeyCheck::Unknown,
                notes: vec![Note::Cancelled],
            })
        })
        .collect()
}

async fn check_one(dialer: &Dialer, spec: &TargetSpec) -> CheckResult {
    let mut r = CheckResult {
        target: spec.id,
        reached: false,
        key: KeyCheck::Unknown,
        notes: vec![],
    };
    let io = match dialer.dial(&spec.url).await {
        Ok(io) => io,
        Err(DialFailure::Unreachable) => return fail(r, Note::Unreachable),
        Err(DialFailure::Tls) => return fail(r, Note::TlsFailed),
    };
    r.reached = true;
    let mut client = match Client::open(io, dialer.limits()).await {
        Ok(c) => c,
        Err(e) => return fail(r, e.note()),
    };
    match client.connect(spec.url.app(), &spec.url.to_string()).await {
        Ok(true) => {}
        Ok(false) => return fail(r, Note::ConnectRefused),
        Err(e) => return fail(r, e.note()),
    }
    let Some(key) = &spec.key else {
        r.notes.push(Note::NoKeyToTry);
        client.close().await;
        return r;
    };
    if !spec.url.is_secure() {
        r.notes.push(Note::KeyTravelsReadable);
    }
    match client.publish(key.expose()).await {
        Ok(PublishOutcome::Accepted) => {
            r.key = KeyCheck::Accepted;
            r.notes.push(Note::KeyAccepted);
        }
        Ok(PublishOutcome::Refused) => {
            r.key = KeyCheck::Rejected;
            r.notes.push(Note::PublishRefused);
        }
        Err(e) => r.notes.push(e.note()),
    }
    client.close().await;
    r
}

fn fail(mut r: CheckResult, note: Note) -> CheckResult {
    r.notes.push(note);
    r
}
