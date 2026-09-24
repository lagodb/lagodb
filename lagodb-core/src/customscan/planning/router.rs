//! Provider-side adapter for the runtime-owned relation planning router.

use std::ffi::c_void;
use std::mem::size_of;
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::gucs;
use crate::customscan::planning::candidate::CustomScanCandidate;
use crate::customscan::planning::paths::CustomScanPathPlanner;
use crate::customscan::provider::{RelationContext, find_matching_provider};
use crate::customscan::{ScanPurpose, has_modify_provider_for};
use crate::runtime_api::{
    CallbackErrorReport, RelationScanPlannerDescriptor, RoutedRelationInfo,
    RoutedRelationScanPlanner,
};

use super::ProviderPartitionedTablePlanner;

pub(crate) fn register() {
    crate::hooks::register_relation_scan(RelationScanPlannerDescriptor {
        struct_size: size_of::<RelationScanPlannerDescriptor>() as u32,
        context: ptr::null_mut(),
        relation_info: Some(relation_info),
        plan_relation: Some(plan_relation),
    });
}

unsafe extern "C-unwind" fn relation_info(
    _context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    relation_oid: pg_sys::Oid,
    _inhparent: bool,
    rel: *mut pg_sys::RelOptInfo,
    error: *mut CallbackErrorReport,
) -> u32 {
    let operation = || {
        // SAFETY: get_relation_info has initialized catalog fields and attribute
        // arrays, but PostgreSQL has not started baserel sizing or path costing.
        unsafe { ProviderPartitionedTablePlanner::prepare(root, relation_oid, rel) }
            .map_err(CustomScanError::into_report_error)
    };
    // SAFETY: the exact-build error record is consumed synchronously by base.
    unsafe { (&mut *error).capture(operation) }
}

const _: RoutedRelationInfo = relation_info;

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
    let candidate = match unsafe { CustomScanCandidate::inspect(root, rel, rte) } {
        Ok(candidate) => candidate,
        Err(rejection) => {
            if unsafe { (*rte).rtekind } == pg_sys::RTEKind::RTE_RELATION
                && unsafe { (*rte).relkind } as u8
                    == pg_sys::RELKIND_PARTITIONED_TABLE
            {
                let context = RelationContext::from_ref(unsafe { &*rte });
                if let Some(provider) = find_matching_provider(&context)?
                    && provider.owns_partitioned_table()
                {
                    return Err(CustomScanError::partitioned_table_rejected(
                        provider.name(),
                        rejection,
                    ));
                }
            }
            return Ok(());
        }
    };
    // SAFETY: `candidate` was just validated from the same live planner
    // structures and no pointer is retained beyond this callback.
    let ctx = unsafe { candidate.relation_context() };

    let provider = match find_matching_provider(&ctx)? {
        Some(provider) => provider,
        None => return Ok(()),
    };

    let is_partitioned_table = ctx.relkind() == pg_sys::RELKIND_PARTITIONED_TABLE;
    if is_partitioned_table && !provider.owns_partitioned_table() {
        return Ok(());
    }
    let owns_partitioned_table =
        is_partitioned_table && provider.owns_partitioned_table();

    if owns_partitioned_table && unsafe { pg_sys::is_dummy_rel(rel) } {
        // Provider-owned partitioned tables undergo plain baserel sizing. A dummy
        // path is a real constraint-exclusion result and must stay empty.
        return Ok(());
    }

    if candidate.purpose() == ScanPurpose::Read
        && !gucs::enabled()
        && !owns_partitioned_table
    {
        if provider.suppress_table_am_parallel_scan() {
            unsafe { suppress_tableam_partial_seqscans(candidate.rel()) };
        }
        return Ok(());
    }

    if candidate.purpose() == ScanPurpose::ModifyTarget {
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

    let rel = candidate.rel();
    let original_paths = unsafe { (*rel).pathlist };
    let original_partial = unsafe { (*rel).partial_pathlist };
    if owns_partitioned_table {
        // Plain storage sizing lets PG generate native paths. This partitioned
        // table's storage is provider-owned, so replace both native path lists.
        unsafe {
            (*rel).pathlist = ptr::null_mut();
            (*rel).partial_pathlist = ptr::null_mut();
        }
    }

    // SAFETY: the validated candidate and registered provider remain live for
    // the synchronous planner operation.
    let mut planner = unsafe { CustomScanPathPlanner::new(candidate, provider) }?;
    if provider.suppress_table_am_parallel_scan() {
        unsafe { suppress_tableam_partial_seqscans(rel) };
    }
    // SAFETY: the planner was constructed for the current live relation.
    let emitted = unsafe { planner.emit() }?;
    if owns_partitioned_table && emitted == 0 {
        unsafe {
            (*rel).pathlist = original_paths;
            (*rel).partial_pathlist = original_partial;
        }
        return Err(CustomScanError::required_partitioned_table_path(
            provider.name(),
        ));
    }
    Ok(())
}

/// Remove only standard partial SeqScan paths, which would invoke the table
/// AM's parallel callbacks. Other partial alternatives (for example parallel
/// index scans) remain available to PostgreSQL.
///
/// # Safety
///
/// `rel` must be the live base relation currently being populated by the
/// set-rel-pathlist hook.
unsafe fn suppress_tableam_partial_seqscans(rel: *mut pg_sys::RelOptInfo) {
    let paths = unsafe { (*rel).partial_pathlist };
    let count = unsafe { pg_sys::list_length(paths) };
    let mut retained: *mut pg_sys::List = ptr::null_mut();
    for index in 0..count {
        let path = unsafe { pg_sys::list_nth(paths, index) }.cast::<pg_sys::Path>();
        if unsafe { (*path).pathtype } == pg_sys::NodeTag::T_SeqScan {
            continue;
        }
        retained = unsafe { pg_sys::lappend(retained, path.cast()) };
    }
    unsafe { (*rel).partial_pathlist = retained };
}
