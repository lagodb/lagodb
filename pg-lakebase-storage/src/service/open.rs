//! OPEN command handler.
//!
//! Combines the attached physical backend identity with `(bucket, key)`, validates cache paths,
//! and then either
//! binds a cached [`Residency`] or admits a fresh one (small-KV via a single insert-if-absent
//! transaction, or a large-fill lease). The result is materialized as an `OpenFileState` slot in
//! the connection's [`HandleTable`], plus an optional direct-IO file attachment when the backing
//! object is fully cached as a complete file.
//!
//! Concurrent OPENs on the same missing key are coordinated through the per-object
//! establishment single-flight (see [`crate::cache::establish`]): exactly one caller becomes the
//! leader and drives HEAD + GET + admit, every other caller awaits the leader's outcome and
//! re-enters the lookup loop — guaranteed to observe a hit on success, given the leader's
//! ordering contract (residency installed **before** outcome published).

use std::sync::Arc;

use tracing::info;

use crate::backend::ObjectBackend;
use crate::cache::{
    CacheIndex, EstablishLeader, OpenOutcome, Residency, ResidencyStateHint,
};
use crate::error::StorageResult;
use crate::object::ObjectLocation;
use crate::service::StorageService;
use crate::service::command::OpenCommand;
use crate::service::reply::{
    CommandOutput, OpenOutput, ResponseAttachment, ServiceReply,
};
use crate::session::StorageContext;
use crate::session::handle_table::{HandleTable, ReservedOpen};

impl<I: CacheIndex> StorageService<I> {
    pub(super) async fn handle_open(
        &self,
        context: &StorageContext<I>,
        command: OpenCommand,
    ) -> StorageResult<ServiceReply> {
        let attached = context.attached()?;
        let key = ObjectLocation::new(
            attached.identity().clone(),
            command.bucket,
            command.key,
        )?;
        let backend = attached.backend();
        self.cache.validate_file_cache_paths(&key)?;
        let open_slot = context.handles.reserve_open()?;

        let residency = self.establish_residency(&key, &backend).await?;

        self.bind_residency(
            &context.handles,
            open_slot,
            key,
            backend,
            command.flags,
            residency,
        )
        .await
    }

    /// Drives the lookup / single-flight loop that turns a key into a [`Residency`].
    ///
    /// Three outcomes are possible per iteration:
    ///
    /// * `Hit` — bind the existing residency and return.
    /// * `Establish(leader)` — this caller is the elected leader. Run HEAD + admit, finalize
    ///   the leader on success (publishing the outcome **after** the residency is visible) or
    ///   failure (publishing the equivalent error for every follower), and return.
    /// * `Waiting(waiter)` — await the current leader. On success the residency is now
    ///   observable; loop back to `lookup_for_open`, where the next pass typically hits the
    ///   `Hit` branch. On failure, surface the leader's error to this caller unchanged.
    ///
    /// # Termination
    ///
    /// Under a quiescent key the loop terminates in at most two iterations: one `Waiting`
    /// (or direct `Establish`) followed by a `Hit`. Progress in the presence of an external
    /// `invalidate_object_cache` that races the retry is still guaranteed, but may take
    /// additional iterations — a follower that wakes on `Succeeded` can observe the residency
    /// has already been invalidated, re-enter `lookup_for_open`, and become the next leader.
    /// Each such bounce consumes exactly one externally-initiated invalidate, so the number
    /// of iterations is bounded by the number of concurrent invalidations, not unbounded
    /// retries against a stable state.
    ///
    /// A failed leader drops the establishment slot, so a subsequent retry would elect a new
    /// leader — but followers do not retry on failure by design, matching the "network error
    /// returns to all concurrent callers" behaviour.
    async fn establish_residency(
        &self,
        key: &ObjectLocation,
        backend: &Arc<dyn ObjectBackend>,
    ) -> StorageResult<Residency> {
        loop {
            match self.cache.lookup_for_open(key).await? {
                OpenOutcome::Hit(residency) => return Ok(residency),
                OpenOutcome::Waiting(waiter) => {
                    waiter.wait().await?;
                }
                OpenOutcome::Establish(leader) => {
                    return self.populate_as_leader(backend, leader).await;
                }
            }
        }
    }

    /// Runs the HEAD + GET + admit path as the elected leader.
    ///
    /// Takes ownership of the leader and finalizes it via `succeed` (after the residency is
    /// observable) or `fail` (propagating the original error). Both finalizers unblock every
    /// outstanding follower before this call returns.
    ///
    /// # Outcome-ordering contract (load-bearing)
    ///
    /// The `Ok(residency)` arm **must not** perform any additional fallible or awaiting
    /// step between [`Self::run_establishment`] returning and `leader.succeed()` being
    /// called. The single-flight guarantees followers that a `Succeeded` signal implies the
    /// residency is observable to a retried `lookup_for_open`; any work that could fail or
    /// delay after `Ok` would break that guarantee and force followers into degraded retry
    /// paths. If a later change needs to do post-admit work, do it **before** establishment
    /// (outside this function) or move it out of the leader path entirely.
    ///
    /// See [`crate::cache::establish`] for the broader single-flight contract.
    async fn populate_as_leader(
        &self,
        backend: &Arc<dyn ObjectBackend>,
        leader: EstablishLeader,
    ) -> StorageResult<Residency> {
        let result = self.run_establishment(backend, &leader).await;
        match &result {
            Ok(_) => leader.succeed(),
            Err(error) => leader.fail(error),
        }
        result
    }

    async fn run_establishment(
        &self,
        backend: &Arc<dyn ObjectBackend>,
        leader: &EstablishLeader,
    ) -> StorageResult<Residency> {
        let key = leader.key();
        let info = backend.head(key.path()).await?;
        if info.size <= self.cache.small_object_limit() {
            // An empty object has no valid byte range. Some object-store
            // implementations reject 0..0 instead of returning an empty body,
            // so admit the metadata directly without issuing a backend GET.
            let data = if info.size == 0 {
                bytes::Bytes::new()
            } else {
                backend.get_range(key.path(), 0..info.size).await?
            };
            self.cache.admit_small(leader, data.to_vec(), info).await
        } else {
            self.cache.admit_large(leader, info).await
        }
    }

    async fn bind_residency(
        &self,
        handles: &HandleTable,
        open_slot: crate::session::handle_table::OpenHandleSlot,
        key: ObjectLocation,
        backend: Arc<dyn ObjectBackend>,
        flags: crate::handle::OpenFlags,
        residency: Residency,
    ) -> StorageResult<ServiceReply> {
        let size = residency.size();
        let hint = residency.state_hint();
        let direct_file = match hint {
            ResidencyStateHint::CompleteFile => {
                Some(self.cache.open_complete_file(&key).await?)
            }
            ResidencyStateHint::SmallKv | ResidencyStateHint::LargeFill => None,
        };
        let direct_io = direct_file.is_some();
        let residency = Arc::new(residency);
        let etag = match &residency.body {
            crate::cache::ResidencyBody::Small { meta, .. }
            | crate::cache::ResidencyBody::Complete { meta } => {
                meta.etag().map(str::to_string)
            }
            crate::cache::ResidencyBody::LargeFill { session } => session.info().etag,
        };
        let state = handles.open_reserved(ReservedOpen {
            slot: open_slot,
            key,
            backend,
            info: crate::object::ObjectInfo { size, etag },
            flags,
            residency: Some(residency),
        });
        info!(
            handle = state.handle.0,
            key = %state.key,
            size = state.size,
            direct_io,
            "handle opened",
        );
        let output = CommandOutput::Open(OpenOutput {
            handle: state.handle,
            size: state.size,
            direct_io,
        });
        match direct_file {
            Some(file) => Ok(ServiceReply::with_attachment(
                output,
                ResponseAttachment::File(file),
            )),
            None => Ok(ServiceReply::new(output)),
        }
    }
}
