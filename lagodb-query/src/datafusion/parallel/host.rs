//! PostgreSQL process and mapping services; no plan or provider ownership.

use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::TableScanRoute;

use crate::datafusion::WorkerTableScanCallbacks;

/// Host-owned snapshot restored when a parallel interrupt hold ends.
#[derive(Clone, Copy)]
pub struct InterruptHoldState(u32);

impl InterruptHoldState {
    /// Capture the host's state before it acquires the parallel-runtime hold.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Return the state that the host must restore when the hold ends.
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Services called exclusively by the participant's current-thread runtime.
/// Implementations are shared with transport handles, which require Send/Sync;
/// they must not transfer PostgreSQL calls to another thread.
pub trait ParallelExecutionHost: Send + Sync {
    fn worker_cap(&self) -> u32;
    fn launch(
        &self,
        workers: u32,
        region_bytes: usize,
    ) -> Result<Option<Box<dyn ParallelWorkers>>, PgReportError>;
    fn receiver_token(&self) -> u64;
    fn wake(&self, token: u64);
    fn interrupt_pending(&self) -> bool;
    fn hold_interrupts(&self) -> InterruptHoldState;
    fn restore_interrupts(&self, state: InterruptHoldState);
    fn process_interrupts(&self) -> Result<(), PgReportError>;
}

/// Stack-owned interrupt hold around `Runtime::block_on`.
///
/// PostgreSQL `ERROR` does not bypass this `Drop` when it originates in a
/// `pg_sys` call: pgrx wraps those calls, catches PostgreSQL's `longjmp`, and
/// resumes it as a Rust panic. PostgreSQL resets `InterruptHoldoffCount` before
/// that unwind, while a direct Rust panic leaves the incremented count intact.
/// The host-owned snapshot lets `Drop` restore the entry state in both cases
/// instead of assuming the current counter can always be decremented.
pub(in crate::datafusion) struct ParallelInterruptGuard<
    'a,
    H: ParallelExecutionHost + ?Sized,
> {
    host: &'a H,
    state: InterruptHoldState,
}

impl<'a, H: ParallelExecutionHost + ?Sized> ParallelInterruptGuard<'a, H> {
    pub(in crate::datafusion) fn new(host: &'a H) -> Self {
        let state = host.hold_interrupts();
        Self { host, state }
    }
}

impl<H: ParallelExecutionHost + ?Sized> Drop for ParallelInterruptGuard<'_, H> {
    fn drop(&mut self) {
        self.host.restore_interrupts(self.state);
    }
}

/// A PG-attached but activation-gated worker group.
///
/// "Attached" has PostgreSQL's `WaitForParallelWorkersToAttach` meaning; it does
/// not promise that every worker has entered the extension. Activation must
/// therefore publish a durable state that a later entrant can observe. The
/// engine initializes its transport before activation and releases transport
/// handles before destroying this owner.
pub trait ParallelWorkers {
    fn region(&self) -> NonNull<c_void>;
    fn attached_workers(&self) -> u32;
    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError>;
    fn activate(&mut self, region_bytes: usize);
    fn finish(&mut self) -> Result<(), PgReportError>;
    /// Relinquish the context to PostgreSQL's transaction-abort cleanup without
    /// waiting in a ResourceOwner callback. A not-yet-activated group is first
    /// released from its activation gate.
    fn abandon(self: Box<Self>);
}

/// Worker-local adapter around the restored PG parallel context.
pub trait ParallelWorkerHost: ParallelExecutionHost {
    fn region(&self) -> NonNull<c_void>;
    fn region_bytes(&self) -> usize;
    fn process_index(&self) -> u32;
    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError>;
    fn resolve_source(
        &self,
        route: TableScanRoute<'_>,
    ) -> Result<WorkerTableScanCallbacks, PgReportError>;
}
