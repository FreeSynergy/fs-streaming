//! `Worker`: a target's task with its queue and counters.
//!
//! Pattern: RAII / Guard — stopping a worker aborts its task, waits for it, and takes its queue out of
//! the hub.

use std::sync::{Arc, Mutex};

use fs_streaming_core::{ObjectId, TargetSpec, TargetUrl};

use crate::fanout::{Branch, Hub};
use crate::relay::Shared;
use crate::target::{TargetCtx, run};
use crate::task::Task;

/// A running (or finished) target worker.
pub struct Worker {
    pub id: ObjectId,
    pub url: TargetUrl,
    pub spec: Arc<Mutex<TargetSpec>>,
    pub branch: Arc<Branch>,
    task: Task,
    hub: Arc<Hub>,
    finished: Arc<std::sync::atomic::AtomicBool>,
}

impl Worker {
    /// Start sending `spec` from `hub`.
    pub fn start(spec: TargetSpec, hub: Arc<Hub>, shared: &Arc<Shared>) -> Self {
        let (id, url) = (spec.id, spec.url.clone());
        let spec = Arc::new(Mutex::new(spec));
        let branch = Branch::new(shared.cfg.queue_bytes);
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ctx = TargetCtx {
            id,
            spec: spec.clone(),
            branch: branch.clone(),
            hub: hub.clone(),
            gate: shared.gate.clone(),
            dialer: shared.dialer.clone(),
            retry: (shared.cfg.retry_min, shared.cfg.retry_max),
        };
        let flag = finished.clone();
        let task = Task::spawn(async move {
            run(ctx).await;
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        });
        Self {
            id,
            url,
            spec,
            branch,
            task,
            hub,
            finished,
        }
    }

    /// The task has ended on its own (an explicit refusal).
    pub fn is_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Replace the spec used from the next connection on.
    pub fn set_spec(&self, spec: TargetSpec) {
        *self
            .spec
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = spec;
    }

    /// End the task and leave the hub.
    pub async fn stop(self) {
        self.hub.remove(&self.branch);
        self.branch.close();
        self.task.stop().await;
    }
}
