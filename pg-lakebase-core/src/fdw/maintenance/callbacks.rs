//! PostgreSQL maintenance callback trampolines.

use core::ffi::c_int;
use core::ptr;

use pgrx::{pg_guard, pg_sys};

use super::contract::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignSampleContext,
    ForeignTruncateContext,
};
use super::error::{ForeignTableMaintenanceError, ForeignTableMaintenancePhase};

#[pg_guard]
/// # Safety
///
/// PostgreSQL must supply the live relation and output pointers required by
/// its `AnalyzeForeignTable` callback contract.
pub(crate) unsafe extern "C-unwind" fn analyze_foreign_table<P: FdwAnalyze>(
    relation: pg_sys::Relation,
    acquire_function: *mut pg_sys::AcquireSampleRowsFunc,
    total_pages: *mut pg_sys::BlockNumber,
) -> bool {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let context = unsafe { ForeignAnalyzeContext::from_raw(relation) };
        let Some(support) = P::analyze(&context)? else {
            return Ok(false);
        };
        unsafe {
            ptr::write(acquire_function, Some(acquire_sample_rows::<P>));
            ptr::write(total_pages, support.total_pages());
        }
        Ok::<bool, ForeignTableMaintenanceError>(true)
    })();

    match result {
        Ok(supported) => supported,
        Err(error) => error
            .with_callback_phase::<P>(ForeignTableMaintenancePhase::Analyze)
            .report_after_switch(prior_context),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL must supply a live relation, a caller-owned sample array with
/// `target_rows` entries, and live population output pointers.
pub(crate) unsafe extern "C-unwind" fn acquire_sample_rows<P: FdwAnalyze>(
    relation: pg_sys::Relation,
    log_level: c_int,
    rows: *mut pg_sys::HeapTuple,
    target_rows: c_int,
    total_rows: *mut f64,
    total_dead_rows: *mut f64,
) -> c_int {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let mut context = unsafe {
            ForeignSampleContext::from_raw(relation, log_level, rows, target_rows)
        };
        let statistics = P::acquire_sample_rows(&mut context)?.validate()?;
        unsafe {
            ptr::write(total_rows, statistics.total_rows());
            ptr::write(total_dead_rows, statistics.total_dead_rows());
        }
        Ok::<c_int, ForeignTableMaintenanceError>(context.commit())
    })();

    match result {
        Ok(sampled_rows) => sampled_rows,
        Err(error) => error
            .with_callback_phase::<P>(ForeignTableMaintenancePhase::AcquireSampleRows)
            .report_after_switch(prior_context),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL must supply its live same-server relation list and valid
/// `ExecForeignTruncate` options.
pub(crate) unsafe extern "C-unwind" fn exec_foreign_truncate<P: FdwTruncate>(
    relations: *mut pg_sys::List,
    behavior: pg_sys::DropBehavior::Type,
    restart_sequences: bool,
) {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = {
        let context = unsafe {
            ForeignTruncateContext::from_raw(relations, behavior, restart_sequences)
        };
        P::truncate(&context)
    };

    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignTableMaintenancePhase::Truncate)
            .report_after_switch(prior_context);
    }
}
