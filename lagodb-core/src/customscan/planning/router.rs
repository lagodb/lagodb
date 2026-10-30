//! Provider-side adapter for the runtime-owned relation planning router.

use std::ffi::c_void;
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::gucs;
use crate::customscan::planning::candidate::CustomScanCandidate;
use crate::customscan::planning::paths::CustomScanPathPlanner;
use crate::customscan::provider::{TableAmSeqScan, find_matching_provider};
use crate::customscan::{ScanPurpose, has_modify_provider_for};
use crate::runtime_api::{
    CallbackErrorReport, RelationScanPlannerDescriptor, RoutedRelationScanPlanner,
};

pub(crate) fn register() {
    crate::hooks::register_relation_scan(RelationScanPlannerDescriptor {
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
    error: *mut CallbackErrorReport,
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
    let Ok(candidate) = (unsafe { CustomScanCandidate::inspect(root, rel, rte) })
    else {
        return Ok(());
    };
    // SAFETY: `candidate` was just validated from the same live planner
    // structures and no pointer is retained beyond this callback.
    let ctx = unsafe { candidate.relation_context() };

    let provider = match find_matching_provider(&ctx)? {
        Some(provider) => provider,
        None => return Ok(()),
    };

    let replaces_seq_scan =
        provider.table_am_seq_scan() == TableAmSeqScan::ReplaceWhenProvided;
    if candidate.purpose() == ScanPurpose::Read {
        if replaces_seq_scan {
            // Only the SeqScans leave: other partial paths (for example
            // parallel index scans) remain available to PostgreSQL.
            unsafe { SeqScans::detach(&mut (*rel).partial_pathlist) };
        }
        if !gucs::enabled() {
            return Ok(());
        }
    }

    if candidate.purpose() == ScanPurpose::ModifyTarget {
        if !has_modify_provider_for(&ctx) {
            return Ok(());
        }

        // Constraint exclusion can replace a target with an empty Result.
        if unsafe { pg_sys::is_dummy_rel(rel) } {
            return Ok(());
        }
        let require_custom = gucs::force_mode();
        // Only SeqScan has an explicit mutation-session binding. Do not
        // expose unrelated native scan methods by restoring all paths.
        let seqscans = unsafe { SeqScans::detach(&mut (*rel).pathlist) };
        // Modification scans are serial.
        unsafe {
            (*rel).pathlist = if require_custom {
                ptr::null_mut()
            } else {
                seqscans.0
            };
            (*rel).partial_pathlist = ptr::null_mut();
        }
        if require_custom || gucs::enabled() {
            let mut planner =
                unsafe { CustomScanPathPlanner::new(candidate, provider) }?;
            unsafe { planner.emit() }?;
        }
        if unsafe { (*rel).pathlist.is_null() } {
            return Err(CustomScanError::required_modify_path(provider.name()));
        }
        return Ok(());
    }

    // SAFETY: the validated candidate and registered provider remain live for
    // the synchronous planner operation.
    let mut planner = unsafe { CustomScanPathPlanner::new(candidate, provider) }?;
    // The SeqScan leaves before the provider path is added: `add_path` frees
    // a new path that an existing one dominates.
    let replaced =
        replaces_seq_scan.then(|| unsafe { SeqScans::detach(&mut (*rel).pathlist) });
    // SAFETY: the planner was constructed for the current live relation.
    if !unsafe { planner.emit() }?
        && let Some(replaced) = replaced
    {
        unsafe { replaced.restore(rel) };
    }
    Ok(())
}

/// The SeqScan paths detached from a relation's path list, in list order.
struct SeqScans(*mut pg_sys::List);

impl SeqScans {
    /// Detach the SeqScan paths of `paths`, keeping the others in order.
    ///
    /// # Safety
    ///
    /// `paths` must be a live planner-owned `List<Path>` (NIL allowed).
    unsafe fn detach(paths: &mut *mut pg_sys::List) -> Self {
        let mut retained = ptr::null_mut();
        let mut seqscans = ptr::null_mut();
        for index in 0..unsafe { pg_sys::list_length(*paths) } {
            let path = unsafe { pg_sys::list_nth(*paths, index) };
            let target = if unsafe { (*path.cast::<pg_sys::Path>()).pathtype }
                == pg_sys::NodeTag::T_SeqScan
            {
                &mut seqscans
            } else {
                &mut retained
            };
            *target = unsafe { pg_sys::lappend(*target, path) };
        }
        *paths = retained;
        Self(seqscans)
    }

    /// Add the detached paths back to `rel` through `add_path`.
    ///
    /// # Safety
    ///
    /// `rel` must be the live relation whose path list they were detached
    /// from, still being populated by the set-rel-pathlist hook.
    unsafe fn restore(self, rel: *mut pg_sys::RelOptInfo) {
        for index in 0..unsafe { pg_sys::list_length(self.0) } {
            let path = unsafe { pg_sys::list_nth(self.0, index) };
            unsafe { pg_sys::add_path(rel, path.cast()) };
        }
    }
}
