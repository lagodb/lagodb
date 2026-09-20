//! PG launch, latch and interrupt adapters for the query engine.

mod entry;
mod workers;

use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::TableScanRoute;
use lagodb_query::datafusion::{
    InterruptHoldState, ParallelExecutionHost, ParallelWorkerHost, ParallelWorkers,
    WorkerTableScanCallbacks,
};
use pgrx::{PgSqlErrorCode, PgTryBuilder, pg_sys};

use super::table_scan_registry::TableScanRegistry;
use workers::{PgParallelWorkers, WorkerMapping};

pub(super) struct PgParallelHost {
    worker: Option<WorkerMapping>,
}

// SAFETY: only the backend's current-thread runtime calls these adapters.
// No PostgreSQL pointer or operation is transferred to a spawned OS thread.
unsafe impl Send for PgParallelHost {}
unsafe impl Sync for PgParallelHost {}

impl PgParallelHost {
    pub(super) fn leader() -> Arc<Self> {
        Arc::new(Self { worker: None })
    }

    fn capture<T>(operation: impl FnOnce() -> T) -> Result<T, PgReportError> {
        PgTryBuilder::new(AssertUnwindSafe(|| Ok(operation())))
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute()
    }

    fn worker(&self) -> &WorkerMapping {
        self.worker
            .as_ref()
            .expect("worker host is constructed by the PG entrypoint")
    }
}

impl ParallelExecutionHost for PgParallelHost {
    fn worker_cap(&self) -> u32 {
        // A PG worker cannot recursively launch another parallel context.
        // A leader already in parallel mode can: CreateParallelContext expects
        // parallel mode; this mixed Gather + leader-owned parallel-execution
        // shape remains supported.
        // SAFETY: backend-local GUC and transaction state on the backend thread.
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
        // SAFETY: PG assigns MyProcNumber before calling a query or worker entrypoint.
        unsafe {
            ((pg_sys::MyProcPid as u32 as u64) << 32)
                | (pg_sys::MyProcNumber as u32 as u64)
        }
    }

    fn wake(&self, token: u64) {
        let index = token as u32 as usize;
        let pid = (token >> 32) as u32 as i32;
        // SAFETY: PG owns the allProcs array for the lifetime of this backend.
        // The shared token is an external transport address; validate its index
        // and pid so recycling a PGPROC slot cannot wake another backend.
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

    fn interrupt_pending(&self) -> bool {
        // SAFETY: signal flags are read only on the backend main thread.
        unsafe { pg_sys::QueryCancelPending != 0 || pg_sys::ProcDiePending != 0 }
    }

    fn hold_interrupts(&self) -> InterruptHoldState {
        // SAFETY: mirrors HOLD_INTERRUPTS on the backend main thread and saves
        // the state owned by surrounding PostgreSQL scopes before acquiring
        // this parallel-runtime hold.
        unsafe {
            let state = InterruptHoldState::new(pg_sys::InterruptHoldoffCount);
            pg_sys::InterruptHoldoffCount += 1;
            state
        }
    }

    fn restore_interrupts(&self, state: InterruptHoldState) {
        // SAFETY: the matching guard is dropping on the same backend thread.
        // PostgreSQL may have reset the counter while raising ERROR, so restore
        // the entry state instead of decrementing the current value.
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
