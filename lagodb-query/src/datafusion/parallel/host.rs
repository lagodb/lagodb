//! PostgreSQL process and mapping services; no plan or provider ownership.

use std::ffi::c_void;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use lagodb_core::backend::SignalWakeup;
use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::TableScanRoute;

use crate::datafusion::scan_callbacks::WorkerTableScanCallbacks;

/// Host-owned snapshot restored when a parallel interrupt hold ends.
#[derive(Clone, Copy)]
pub struct InterruptHoldState(u32);

impl InterruptHoldState {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u32 {
        self.0
    }
}

/// What a parked participant must react to.
///
/// A pending parallel message is serviced by the leader and the wait resumes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParallelInterrupt {
    None,
    Service,
    Abort,
}

/// Services called exclusively by the participant's current-thread runtime.
///
/// They are shared with transport handles, which require Send/Sync, so they must
/// not transfer PostgreSQL calls to another thread.
pub trait ParallelExecutionHost: Send + Sync {
    fn worker_cap(&self) -> u32;
    fn launch(
        &self,
        workers: u32,
        region_bytes: usize,
    ) -> Result<Option<Box<dyn ParallelWorkers>>, PgReportError>;
    fn receiver_token(&self) -> u64;
    fn wake(&self, token: u64);
    fn interrupt(&self) -> ParallelInterrupt;
    fn hold_interrupts(&self) -> InterruptHoldState;
    fn restore_interrupts(&self, state: InterruptHoldState);
    fn process_interrupts(&self) -> Result<(), PgReportError>;

    /// This process's `SIGURG` listener, created on first use.
    fn acquire_wakeup(&self) -> Result<&'static SignalWakeup, PgReportError>;

    /// Withdraws the park announcement after a `block_on` ends for any reason; a
    /// panic inside the park unwinds past the unpark hook. Must not consume the
    /// latch.
    fn end_park(&self);

    /// The postmaster's watch descriptor, owned by PostgreSQL.
    fn postmaster_fd(&self) -> RawFd;

    /// Reserves the descriptors one IO-enabled runtime adds.
    fn acquire_runtime_fds(&self) -> Result<(), PgReportError>;

    fn release_runtime_fds(&self);

    /// Whether this process can be woken from a park right now; `false` only
    /// means this statement runs serially.
    fn wakeup_available(&self) -> bool {
        self.acquire_wakeup()
            .is_ok_and(|signal| signal.verify().is_ok())
    }
}

/// Stack-owned hold around `Runtime::block_on`.
///
/// A PostgreSQL `ERROR` becomes a Rust panic after PostgreSQL has already reset
/// `InterruptHoldoffCount`, while a direct Rust panic leaves it raised, so `Drop`
/// restores the captured entry state and withdraws a park announcement the same
/// unwind can leave behind.
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
        // Withdraw the park before handing the interrupt state back.
        self.host.end_park();
        self.host.restore_interrupts(self.state);
    }
}

/// A PG-attached but activation-gated worker group.
///
/// "Attached" has `WaitForParallelWorkersToAttach` meaning: activation must
/// publish a durable state that a later entrant can observe.
pub trait ParallelWorkers {
    fn region(&self) -> NonNull<c_void>;
    fn attached_workers(&self) -> u32;
    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError>;
    fn activate(&mut self, region_bytes: usize);
    fn finish(&mut self) -> Result<(), PgReportError>;
    /// Relinquish the context to PostgreSQL's transaction-abort cleanup without
    /// waiting in a ResourceOwner callback.
    fn abandon(self: Box<Self>);
}

/// Worker-local adapter around the restored PG parallel context.
pub trait ParallelWorkerHost: ParallelExecutionHost {
    fn region(&self) -> NonNull<c_void>;
    /// Mapped bytes published by the leader: prefix plus inventory tail.
    fn region_bytes(&self) -> usize;
    fn process_index(&self) -> u32;
    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError>;
    fn resolve_source(
        &self,
        route: TableScanRoute<'_>,
    ) -> Result<WorkerTableScanCallbacks, PgReportError>;
}
