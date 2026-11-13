//! PG launch, latch and interrupt adapters for the query engine.

mod entry;
mod workers;

use std::ffi::c_void;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use lagodb_core::backend::{BackendLatch, BackendProcess, SignalWakeup};
use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::TableScanRoute;
use lagodb_query::datafusion::{
    InterruptHoldState, ParallelExecutionHost, ParallelInterrupt, ParallelWorkerHost,
    ParallelWorkers, WorkerTableScanCallbacks,
};
use pgrx::{PgSqlErrorCode, pg_sys};

use super::table_scan_registry::TableScanRegistry;
use workers::{PgParallelWorkers, WorkerMapping};

/// Descriptors one IO-enabled runtime adds besides [`TOKIO_SIGNAL_FDS`]: the
/// epoll/kqueue selector, its waker (an eventfd on Linux, a kqueue duplicate on
/// macOS), the registry duplicate the IO driver keeps, and the signal receiver
/// duplicate.
const RUNTIME_FDS: usize = 4;

/// Descriptors Tokio's process-wide signal pair owns.
///
/// lagodb-query enables Tokio's `signal` feature, so the first IO-enabled
/// runtime creates the pair inside `Runtime::build` and keeps it until exit; it
/// is reserved once beforehand and never released.
const TOKIO_SIGNAL_FDS: usize = 2;

/// Set once [`TOKIO_SIGNAL_FDS`] is reserved for this process.
static SIGNAL_PAIR_RESERVED: AtomicBool = AtomicBool::new(false);

pub(super) struct PgParallelHost {
    worker: Option<WorkerMapping>,
}

impl PgParallelHost {
    pub(super) fn leader() -> Arc<Self> {
        Arc::new(Self { worker: None })
    }

    fn capture<T>(operation: impl FnOnce() -> T) -> Result<T, PgReportError> {
        PgReportError::capture(operation)
    }

    fn worker(&self) -> &WorkerMapping {
        self.worker
            .as_ref()
            .expect("worker host is constructed by the PG entrypoint")
    }

    /// Reserves `count` descriptors, releasing a partial reservation.
    fn reserve_fds(count: usize) -> Result<(), PgReportError> {
        for reserved in 0..count {
            // SAFETY: backend-local fd.c accounting, on the backend thread.
            if unsafe { pg_sys::AcquireExternalFD() } {
                continue;
            }
            // SAFETY: pairs with the reservations this loop already made.
            for _ in 0..reserved {
                unsafe { pg_sys::ReleaseExternalFD() };
            }
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                "PostgreSQL external file descriptor budget exhausted",
            ));
        }
        Ok(())
    }

    /// Reserves the process-wide signal pair once, before any IO runtime exists.
    ///
    /// A failed attempt releases everything it took and leaves the flag clear.
    fn reserve_signal_pair() -> Result<(), PgReportError> {
        if SIGNAL_PAIR_RESERVED.load(Ordering::Acquire) {
            return Ok(());
        }
        Self::reserve_fds(TOKIO_SIGNAL_FDS)?;
        SIGNAL_PAIR_RESERVED.store(true, Ordering::Release);
        Ok(())
    }
}

// SAFETY: only the backend's current-thread runtime calls these adapters.
// No PostgreSQL pointer or operation is transferred to a spawned OS thread.
unsafe impl Send for PgParallelHost {}
unsafe impl Sync for PgParallelHost {}

impl ParallelExecutionHost for PgParallelHost {
    fn worker_cap(&self) -> u32 {
        // A PG worker cannot launch another parallel context; a leader already
        // in parallel mode can, which is the supported Gather + leader-owned
        // parallel-execution shape.
        // SAFETY: backend-local GUCs read on the backend thread.
        unsafe {
            if pg_sys::ParallelWorkerNumber >= 0 {
                return 0;
            }
            pg_sys::max_parallel_workers_per_gather
                .min(pg_sys::max_parallel_workers)
                .min(pg_sys::max_worker_processes) as u32
        }
    }

    fn launch(
        &self,
        workers: u32,
        region_bytes: usize,
    ) -> Result<Option<Box<dyn ParallelWorkers>>, PgReportError> {
        PgParallelWorkers::launch(workers, region_bytes).map(|workers| {
            workers.map(|workers| Box::new(workers) as Box<dyn ParallelWorkers>)
        })
    }

    fn receiver_token(&self) -> u64 {
        // SAFETY: PG assigns MyProcPid before calling a query or worker entrypoint.
        let pid = unsafe { pg_sys::MyProcPid };
        ((pid as u32 as u64) << 32) | (BackendProcess::proc_number() as u32 as u64)
    }

    fn wake(&self, token: u64) {
        let index = token as u32 as usize;
        let pid = (token >> 32) as u32 as i32;
        // SAFETY: PG owns the allProcs array for the lifetime of this backend.
        // The token is an external address, so its index and pid are validated
        // to keep a recycled PGPROC slot from waking another backend.
        unsafe {
            let global = pg_sys::ProcGlobal;
            if index < (*global).allProcCount as usize {
                let process = (*global).allProcs.add(index);
                if (*process).pid == pid {
                    pg_sys::SetLatch(&raw mut (*process).procLatch);
                }
            }
        }
    }

    fn interrupt(&self) -> ParallelInterrupt {
        if BackendProcess::query_interrupt_pending() {
            return ParallelInterrupt::Abort;
        }
        // PostgreSQL owns the complete interrupt-reason list. Service every
        // non-aborting reason through ProcessInterrupts instead of duplicating
        // that list here.
        // SAFETY: the flag is backend-local and read on the backend thread.
        if unsafe { pg_sys::InterruptPending } != 0 {
            return ParallelInterrupt::Service;
        }
        ParallelInterrupt::None
    }

    fn acquire_wakeup(&self) -> Result<&'static SignalWakeup, PgReportError> {
        SignalWakeup::acquire().map_err(PgReportError::from_domain_error)
    }

    fn end_park(&self) {
        // The reactor that installed the hold needed this listener, and cleanup
        // must not install or panic in a `Drop` that can run while unwinding.
        if let Some(wakeup) = SignalWakeup::installed() {
            wakeup.end_park();
        }
        BackendLatch::clear_sleep_announcement();
    }

    fn postmaster_fd(&self) -> RawFd {
        // SAFETY: PostgreSQL creates the watch pipe before the backend runs and
        // keeps it open for the backend's life.
        unsafe { pg_sys::postmaster_alive_fds[pg_sys::POSTMASTER_FD_WATCH as usize] }
    }

    fn acquire_runtime_fds(&self) -> Result<(), PgReportError> {
        Self::reserve_signal_pair()?;
        Self::reserve_fds(RUNTIME_FDS)
    }

    fn release_runtime_fds(&self) {
        for _ in 0..RUNTIME_FDS {
            // SAFETY: pairs with `acquire_runtime_fds`.
            unsafe { pg_sys::ReleaseExternalFD() };
        }
    }

    fn hold_interrupts(&self) -> InterruptHoldState {
        // SAFETY: mirrors HOLD_INTERRUPTS on the backend main thread, saving the
        // state owned by the surrounding PostgreSQL scopes.
        unsafe {
            let state = InterruptHoldState::new(pg_sys::InterruptHoldoffCount);
            pg_sys::InterruptHoldoffCount += 1;
            state
        }
    }

    fn restore_interrupts(&self, state: InterruptHoldState) {
        // SAFETY: the matching guard drops on the same backend thread. PostgreSQL
        // may have reset the counter while raising ERROR, so the entry state is
        // restored instead of decrementing.
        unsafe { pg_sys::InterruptHoldoffCount = state.value() };
    }

    fn process_interrupts(&self) -> Result<(), PgReportError> {
        Self::capture(|| {
            pgrx::check_for_interrupts!();
        })
    }
}

impl ParallelWorkerHost for PgParallelHost {
    fn region(&self) -> NonNull<c_void> {
        self.worker().region
    }

    fn region_bytes(&self) -> usize {
        self.worker().region_bytes()
    }

    fn process_index(&self) -> u32 {
        // SAFETY: the PG parallel entrypoint has a dense zero-based worker number.
        unsafe { pg_sys::ParallelWorkerNumber as u32 + 1 }
    }

    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError> {
        self.worker().watch_mapping(alive)
    }

    fn resolve_source(
        &self,
        route: TableScanRoute<'_>,
    ) -> Result<WorkerTableScanCallbacks, PgReportError> {
        TableScanRegistry::resolve_worker_callbacks(route)?.ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "parallel worker route {:?} has no worker callbacks",
                    route.name()
                ),
            )
        })
    }
}
