//! Provider-side adapter for the runtime-owned relation planning router.

use std::ffi::c_void;
use std::mem::size_of;
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::gucs;
use crate::customscan::planning::candidate::CustomScanCandidate;
use crate::customscan::planning::paths::CustomScanPathPlanner;
use crate::customscan::provider::find_matching_provider;
use crate::customscan::{ScanPurpose, has_modify_provider_for};
use crate::runtime_api::{
    PlanErrorRecord, RelationScanPlannerDescriptor, RoutedRelationScanPlanner,
};

pub(crate) fn register() {
    crate::hooks::register_relation_scan(RelationScanPlannerDescriptor {
        struct_size: size_of::<RelationScanPlannerDescriptor>() as u32,
        context: ptr::null_mut(),
        plan_relation: Some(plan_relation),
    });
}

unsafe extern "C-unwind" fn plan_relation(
    _context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
    error: *mut PlanErrorRecord,
) -> u32 {
    let operation = || {
        // SAFETY: PostgreSQL supplies live planner structures for this hook
        // invocation; the runtime forwards them without retaining pointers.
        unsafe { plan_relation_paths(root, rel, rti, rte) }
            .map_err(CustomScanError::into_report_error)
    };
    // SAFETY: the runtime supplies its stack-owned exact-build error record and
    // consumes it synchronously after this callback returns.
    unsafe { (&mut *error).capture(operation) }
}

const _: RoutedRelationScanPlanner = plan_relation;

unsafe fn plan_relation_paths(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    _rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) -> Result<(), CustomScanError> {
    // SAFETY: the runtime invokes this adapter only from PostgreSQL's
    // `set_rel_pathlist_hook` with its live planner-owned arguments.
    let candidate = match unsafe { CustomScanCandidate::inspect(root, rel, rte) } {
        Ok(candidate) => candidate,
        Err(_) => return Ok(()),
    };
    if candidate.purpose() == ScanPurpose::Query && !gucs::enabled() {
        return Ok(());
    }
    // SAFETY: `candidate` was just validated from the same live planner
    // structures and no pointer is retained beyond this callback.
    let ctx = unsafe { candidate.relation_context() };

    let provider = match find_matching_provider(&ctx)? {
        Some(provider) => provider,
        None => return Ok(()),
    };

    if candidate.purpose() == ScanPurpose::Modify {
        if !has_modify_provider_for(&ctx) {
            return Ok(());
        }

        // SAFETY: the candidate owns the live `RelOptInfo` passed to this
        // planning callback.
        let original_paths = unsafe { (*candidate.rel()).pathlist };
        // SAFETY: same live `RelOptInfo` as `original_paths`.
        let original_partial = unsafe { (*candidate.rel()).partial_pathlist };
        // SAFETY: PostgreSQL permits a set-rel hook to replace these path lists
        // while the relation is being planned.
        unsafe {
            (*candidate.rel()).pathlist = ptr::null_mut();
            (*candidate.rel()).partial_pathlist = ptr::null_mut();
        }
        // SAFETY: the validated candidate and registered provider remain live
        // for the synchronous planner operation.
        let mut planner = unsafe { CustomScanPathPlanner::new(candidate, provider) }?;
        // SAFETY: the planner was constructed for the current live relation.
        let emitted = unsafe { planner.emit() }?;
        if emitted == 0 {
            // SAFETY: the relation is still live and no replacement path was
            // emitted, so restore the exact lists saved above.
            unsafe {
                (*candidate.rel()).pathlist = original_paths;
                (*candidate.rel()).partial_pathlist = original_partial;
            }
            return Err(CustomScanError::required_modify_path(provider.name()));
        }
        return Ok(());
    }

    // SAFETY: the validated candidate and registered provider remain live for
    // the synchronous planner operation.
    let mut planner = unsafe { CustomScanPathPlanner::new(candidate, provider) }?;
    // SAFETY: the planner was constructed for the current live relation.
    let _ = unsafe { planner.emit() }?;
    Ok(())
}
