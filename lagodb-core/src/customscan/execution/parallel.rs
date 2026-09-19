//! PostgreSQL native-parallel CustomScan callback boundary.

use core::ffi::c_void;

use pgrx::{pg_guard, pg_sys};

use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::provider::LagodbCustomScanProvider;

use super::state::CustomScanStateWrapper;

fn state<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
) -> Result<&'static mut P::State, CustomScanError> {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    wrapper.provider_state.as_mut().ok_or_else(|| {
        CustomScanError::internal(std::io::Error::other(
            "parallel CustomScan callback ran before provider state initialization",
        ))
    })
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn estimate_dsm<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    match state::<P>(node).and_then(P::estimate_dsm) {
        Ok(size) => size,
        Err(error) => error
            .with_callback_phase(P::NAME, CustomScanPhase::EstimateDsm)
            .report(),
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn initialize_dsm<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut c_void,
) {
    let result = state::<P>(node)
        .and_then(|state| unsafe { P::initialize_dsm(state, coordinate) });
    if let Err(error) = result {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::InitializeDsm)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn reinitialize_dsm<
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut c_void,
) {
    let result = state::<P>(node)
        .and_then(|state| unsafe { P::reinitialize_dsm(state, coordinate) });
    if let Err(error) = result {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::ReInitializeDsm)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn initialize_worker<
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    toc: *mut pg_sys::shm_toc,
    coordinate: *mut c_void,
) {
    let result = state::<P>(node)
        .and_then(|state| unsafe { P::initialize_worker(state, toc, coordinate) });
    if let Err(error) = result {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::InitializeWorker)
            .report();
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn shutdown<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
) {
    let result = state::<P>(node).and_then(P::shutdown_parallel);
    if let Err(error) = result {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::Shutdown)
            .report();
    }
}
