//! The single PG FFI entrypoint for all distributed query shapes.

use std::sync::Arc;

use lagodb_query::datafusion::run_parallel_worker;
use pgrx::{pg_guard, pg_sys};

use super::{PgParallelHost, workers::WorkerMapping};

#[unsafe(no_mangle)]
#[pg_guard]
pub unsafe extern "C-unwind" fn lagodb_query_worker(
    segment: *mut pg_sys::dsm_segment,
    toc: *mut pg_sys::shm_toc,
) {
    // SAFETY: ParallelWorkerMain passes the restored LagoDB parallel TOC.
    let mapping = unsafe { WorkerMapping::attach(segment, toc) };
    let host = Arc::new(PgParallelHost {
        worker: Some(mapping),
    });
    // There is deliberately no extension-entry readiness notification here.
    // The leader uses PostgreSQL's attach barrier for worker startup, while the
    // level-triggered RUN/ABORT state makes entering this gate after a broadcast
    // safe. A second counter/latch handshake would duplicate PG lifecycle state.
    let result = match host.worker().wait_for_activation() {
        Ok(true) => unsafe { run_parallel_worker(Arc::clone(&host)) }
            .map_err(|error| error.into_report()),
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    };
    // Engine plans, readers and runtimes have been released before reporting.
    if let Err(error) = result {
        error.report();
    }
}
