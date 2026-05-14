//! Reaper channel plumbing and task loop.
//!
//! The reaper is the async finalizer that [`Drop`] cannot be: cleaning up an incomplete partial
//! file requires a tokio runtime and the per-object lock. [`super::session::LargeFillSession`]'s
//! [`Drop`] impl pushes a [`ReapRequest`] into an unbounded mpsc; the task launched via
//! [`crate::cache::CacheManager::spawn_large_fill_reaper`] consumes that channel.
//!
//! # State pinning
//!
//! Each [`ReapRequest`] carries the `Arc<PerObjectState>` the session was attached to. This is
//! load-bearing: when the session's last `Arc` drops the per-object state would otherwise become
//! reclaimable before the reaper has had a chance to run, and a concurrent OPEN on the same key
//! could have recreated a fresh state. The `Arc` on the request pins the original state instance
//! through the reap window so the nonce check observes the same fill slot the session registered
//! into.

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use tokio::sync::mpsc;
use tracing::warn;

use crate::cache::object_state::PerObjectState;
use crate::cache::{CacheIndex, CacheManager};

/// Sent from [`super::session::LargeFillSession`]'s [`Drop`] into the reaper task when the last
/// `Arc` goes away and the fill has not committed. Carries everything the reaper needs — the
/// session itself is gone by the time the request lands.
pub(crate) struct ReapRequest {
    /// Pins the per-object state for the whole reap window (see module docs).
    pub(crate) state: Arc<PerObjectState>,
    pub(crate) partial_path: PathBuf,
    /// Session identity used by the reaper to refuse clobbering a newer session that happens to
    /// have re-used the same key after this one died.
    pub(crate) nonce: u64,
}

/// Cheap-to-clone sender half of the reaper channel.
///
/// Held by [`crate::cache::object_state::ObjectStateRegistry`] and cloned into every session so
/// the session's [`Drop`] can enqueue without needing access to [`CacheManager`].
#[derive(Clone)]
pub(crate) struct ReaperHandle {
    tx: mpsc::UnboundedSender<ReapRequest>,
}

impl ReaperHandle {
    fn new(tx: mpsc::UnboundedSender<ReapRequest>) -> Self {
        Self { tx }
    }

    pub(super) fn send(&self, req: ReapRequest) {
        // The only expected failure mode is "receiver closed" — which only happens during
        // CacheManager teardown. In that case orphan scanning picks up any residual partials on
        // the next startup, which is exactly the contract for a shutting-down process.
        if let Err(mpsc::error::SendError(dropped)) = self.tx.send(req) {
            warn!(
                target: "pg_lakebase_storage::cache",
                key = %dropped.state.key(),
                partial = %dropped.partial_path.display(),
                "large-fill reaper channel closed; incomplete partial will be handled by orphan cleanup",
            );
        }
    }
}

/// Per-[`CacheManager`] reaper task state. Owns the receiver half of the channel.
pub(crate) struct ReaperInbox {
    pub(super) rx: mpsc::UnboundedReceiver<ReapRequest>,
}

/// Creates the (handle, inbox) pair. The handle is installed into the registry; the inbox is
/// taken by [`CacheManager::spawn_large_fill_reaper`] when the runtime is ready.
pub(crate) fn reaper_channel() -> (ReaperHandle, ReaperInbox) {
    let (tx, rx) = mpsc::unbounded_channel();
    (ReaperHandle::new(tx), ReaperInbox { rx })
}

pub(super) async fn run_reaper<I: CacheIndex + 'static>(
    mut inbox: ReaperInbox,
    cache: Weak<CacheManager<I>>,
) {
    while let Some(request) = inbox.rx.recv().await {
        let Some(cache) = cache.upgrade() else {
            // Manager is shutting down; drop remaining requests and let orphan cleanup pick up
            // anything we could not finalize.
            break;
        };
        cache.reap_large_fill(request).await;
    }
}
