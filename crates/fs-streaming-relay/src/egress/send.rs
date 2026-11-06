//! The worker loop of one target.
//!
//! Pattern: State (connecting, sending, retrying, refused — reported to the core, which owns the
//! transition function). The queue is a `Branch` of the hub; a lost connection resets it, so the next
//! connection starts with the codec headers and a keyframe. An explicit refusal ends the loop.

use std::sync::{Arc, Mutex};

use fs_streaming_core::{Gate, Note, ObjectId, RelayTargetEvent, TargetSpec};

use super::Backoff;
use crate::fanout::{Branch, Hub};
use crate::wire::{Client, DialFailure, Dialer, PublishOutcome};

/// Everything a worker needs.
pub struct TargetCtx {
    /// The target.
    pub id: ObjectId,
    /// Its address and key; the key may change between connections.
    pub spec: Arc<Mutex<TargetSpec>>,
    /// Its queue.
    pub branch: Arc<Branch>,
    /// Where frames come from.
    pub hub: Arc<Hub>,
    /// Where state goes.
    pub gate: Arc<dyn Gate>,
    /// How to connect.
    pub dialer: Dialer,
    /// Pause limits.
    pub retry: (std::time::Duration, std::time::Duration),
}

enum End {
    Refused(Note),
    Lost { note: Note, reached: bool },
    Done,
}

struct Joined<'a>(&'a TargetCtx);

impl Drop for Joined<'_> {
    fn drop(&mut self) {
        self.0.hub.remove(&self.0.branch);
    }
}

/// Run until refused or told to stop (the owner aborts the task).
pub async fn run(ctx: TargetCtx) {
    let mut backoff = Backoff::new(ctx.retry.0, ctx.retry.1);
    let mut first = true;
    loop {
        ctx.gate
            .target_event(ctx.id, RelayTargetEvent::Connecting)
            .await;
        if !first {
            ctx.branch
                .stats
                .reconnects
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        first = false;
        match attempt(&ctx).await {
            End::Done => return,
            End::Refused(note) => {
                tracing::info!(target_id = %ctx.id, "target refused: {note}");
                ctx.gate
                    .target_event(ctx.id, RelayTargetEvent::Refused(note))
                    .await;
                return;
            }
            End::Lost { note, reached } => {
                if reached {
                    backoff.reset();
                }
                tracing::debug!(target_id = %ctx.id, "target connection lost: {note}");
                ctx.gate
                    .target_event(ctx.id, RelayTargetEvent::Lost(note))
                    .await;
                tokio::time::sleep(backoff.next_pause()).await;
            }
        }
    }
}

fn lost(note: Note) -> End {
    End::Lost {
        note,
        reached: false,
    }
}

async fn attempt(ctx: &TargetCtx) -> End {
    let spec = ctx
        .spec
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let limits = ctx.dialer.limits();
    let io = match ctx.dialer.dial(&spec.url).await {
        Ok(io) => io,
        Err(DialFailure::Unreachable) => return lost(Note::Unreachable),
        Err(DialFailure::Tls) => return lost(Note::TlsFailed),
    };
    let mut client = match Client::open(io, limits).await {
        Ok(c) => c,
        Err(e) => return lost(e.note()),
    };
    let tc_url = spec.url.to_string();
    match client.connect(spec.url.app(), &tc_url).await {
        Ok(true) => {}
        Ok(false) => return End::Refused(Note::ConnectRefused),
        Err(e) => return lost(e.note()),
    }
    let key = spec.key.as_ref().map_or("", |k| k.expose());
    match client.publish(key).await {
        Ok(PublishOutcome::Accepted) => {}
        Ok(PublishOutcome::Refused) => return End::Refused(Note::PublishRefused),
        Err(e) => return lost(e.note()),
    }
    ctx.branch.reset();
    ctx.hub.add(ctx.branch.clone());
    let _joined = Joined(ctx);
    ctx.gate
        .target_event(ctx.id, RelayTargetEvent::Sending)
        .await;
    send_loop(ctx, &mut client).await
}

/// Media must have flowed this long for a loss to start the pauses over.
const STABLE: std::time::Duration = std::time::Duration::from_secs(10);

async fn send_loop(ctx: &TargetCtx, client: &mut Client) -> End {
    let since = std::time::Instant::now();
    let stall = ctx.dialer.limits().write_stall;
    let gone = |note| End::Lost {
        note,
        reached: since.elapsed() >= STABLE,
    };
    loop {
        tokio::select! {
            frame = ctx.branch.next() => {
                let Some(frame) = frame else { return End::Done };
                match tokio::time::timeout(stall, client.send_frame(&frame)).await {
                    Ok(Ok(n)) => { ctx.branch.stats.bytes_out.fetch_add(n, std::sync::atomic::Ordering::Relaxed); }
                    Ok(Err(e)) => return gone(e.note()),
                    Err(_) => return gone(Note::NoAnswer),
                }
            }
            read = client.read_step() => {
                if let Err(e) = read { return gone(e.note()); }
                if client.take_error() { return gone(Note::ProtocolError); }
                if tokio::time::timeout(stall, client.flush()).await.map_or(true, |r| r.is_err()) { return gone(Note::NoAnswer); }
            }
        }
    }
}
