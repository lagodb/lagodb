//! PostgreSQL native-parallel FDW callback boundary.

use core::ffi::c_void;

use pgrx::{pg_guard, pg_sys};

use super::{FdwScan, ForeignScanError, ForeignScanPhase, ForeignScanStateWrapper};

fn wrapper<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
) -> Result<&'static mut ForeignScanStateWrapper<P>, ForeignScanError> {
    let raw = unsafe { (*node).fdw_state };
    if raw.is_null() {
        return Err(ForeignScanError::framework(
            "parallel FDW callback received a null fdw_state",
        ));
    }
    Ok(unsafe { &mut *raw.cast::<ForeignScanStateWrapper<P>>() })
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn is_parallel_safe<P: FdwScan>(
    _root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    _rte: *mut pg_sys::RangeTblEntry,
) -> bool {
    // PostgreSQL calls this while deciding whether to set
    // `rel->consider_parallel`; consulting that field here would be circular
    // and would keep every foreign relation serial.
    P::NATIVE_PARALLEL
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn estimate_dsm<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    _pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    let result = wrapper::<P>(node)
        .and_then(ForeignScanStateWrapper::parallel_state)
        .and_then(|state| {
            if P::parallel_workers_allowed(state) {
                P::estimate_dsm(state)
            } else {
                // PostgreSQL retains the TOC entry, but this scan has no
                // provider DSM payload and never accesses its coordinate.
                Ok(0)
            }
        });
    match result {
        Ok(size) => size,
        Err(error) => error
            .with_callback_phase::<P>(ForeignScanPhase::EstimateDsm)
            .report(),
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn initialize_dsm<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut c_void,
) {
    let result = wrapper::<P>(node)
        .and_then(ForeignScanStateWrapper::parallel_state)
        .and_then(|state| {
            if P::parallel_workers_allowed(state) {
                // SAFETY: PostgreSQL allocated the provider's estimated DSM.
                unsafe { P::initialize_dsm(state, coordinate) }
            } else {
                // SAFETY: PostgreSQL initialized this parallel context and
                // has not launched workers yet.
                unsafe { pg_sys::ReinitializeParallelWorkers(pcxt, 0) };
                Ok(())
            }
        });
    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::InitializeDsm)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn reinitialize_dsm<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut c_void,
) {
    let result = wrapper::<P>(node)
        .and_then(ForeignScanStateWrapper::parallel_state)
        .and_then(|state| {
            if P::parallel_workers_allowed(state) {
                // SAFETY: PostgreSQL retained this scan's initialized DSM.
                unsafe { P::reinitialize_dsm(state, coordinate) }
            } else {
                // SAFETY: PostgreSQL reset this parallel context; workers
                // for the new scan have not been launched yet.
                unsafe { pg_sys::ReinitializeParallelWorkers(pcxt, 0) };
                Ok(())
            }
        });
    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::ReInitializeDsm)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn initialize_worker<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    toc: *mut pg_sys::shm_toc,
    coordinate: *mut c_void,
) {
    let result = wrapper::<P>(node)
        .and_then(ForeignScanStateWrapper::parallel_state)
        .and_then(|state| unsafe { P::initialize_worker(state, toc, coordinate) });
    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::InitializeWorker)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn shutdown_parallel<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
) {
    let result = wrapper::<P>(node)
        .and_then(ForeignScanStateWrapper::parallel_state)
        .and_then(P::shutdown_parallel);
    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::Shutdown)
            .report();
    }
}
