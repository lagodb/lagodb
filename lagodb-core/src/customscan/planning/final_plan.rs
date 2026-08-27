//! Final `PlanCustomPath` negotiation and `CustomScan` node construction.

use core::ffi::{c_int, c_void};
use core::mem::size_of;
use core::ptr;

use pgrx::{pg_guard, pg_sys};

use crate::customscan::error::CustomScanError;
use crate::customscan::filter::CustomScanFilters;
use crate::customscan::plan_data::custom_private::CustomPrivatePlan;
use crate::customscan::plan_data::path_private::decode_path_private;
use crate::customscan::provider::{LagodbCustomScanProvider, method_tables_for};
use crate::diag::ReportableError;
use crate::expr::pushdown::{
    FilterNegotiator, FilterPlanningContext, ScanClauseSource,
};
use crate::expr::relation::PlanRelationResolver;

use super::tuple_planner::{BaseScanTuplePlanner, PlannedScanTuple};

/// Replan the final clauses authoritatively, persist planned filters, and
/// assemble the selected provider's `CustomScan` node.
///
/// # Safety
///
/// Called from `create_customscan_plan`; all planner pointers are live in the
/// per-query memory context. `clauses` is `List<RestrictInfo>` (not extracted).
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn plan_custom_path_trampoline<
    P: LagodbCustomScanProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        plan_custom_path::<P>(root, rel, best_path, tlist, clauses, custom_plans)
    }
    .report_unwrap()
}

unsafe fn plan_custom_path<P: LagodbCustomScanProvider>(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> Result<*mut pg_sys::Plan, CustomScanError> {
    let path_private = unsafe { decode_path_private((*best_path).custom_private) }?;
    let provider_metadata = path_private.provider_metadata;
    let final_clause_sources = unsafe { FinalScanClauseSources::new(rel) };

    let relation_oid =
        unsafe { PlanRelationResolver::new(root).rel_oid((*rel).relid) };
    let planning_context = FilterPlanningContext::new(
        relation_oid,
        unsafe { (*rel).relid },
        unsafe { pg_sys::get_rel_tablespace(relation_oid) },
        unsafe {
            let user = (*rel).userid;
            if user == pg_sys::InvalidOid {
                pg_sys::GetUserId()
            } else {
                user
            }
        },
    );
    let mut filter_planner = P::begin_filter_planning(&planning_context)
        .map_err(CustomScanError::provider)?;
    let filters = unsafe {
        FilterNegotiator::new(&mut filter_planner, relation_oid, rel)
            .negotiate_with_source(clauses, |rinfo| {
                final_clause_sources.source_for(rinfo)
            })
    }
    .map_err(CustomScanError::provider)?;

    let custom_exprs = unsafe {
        let mut list: *mut pg_sys::List = ptr::null_mut();
        for binding in &filters.bindings {
            list = pg_sys::lappend(list, binding.expr.cast::<c_void>());
        }
        for filter in &filters.planned {
            list = pg_sys::lappend(list, filter.pushed_expr.cast::<c_void>());
        }
        list
    };

    let plan_qual = unsafe {
        let mut list: *mut pg_sys::List = ptr::null_mut();
        for &expr in &filters.residual {
            list = pg_sys::lappend(list, expr.cast::<c_void>());
        }
        list
    };

    let tuple_planner =
        BaseScanTuplePlanner::new(unsafe { (*rel).relid }, relation_oid);
    let scan_tuple = if path_private.requires_wholerow {
        PlannedScanTuple::relation()
    } else if path_private.purpose.is_modify() {
        unsafe {
            tuple_planner.plan_relation_scan(
                tlist,
                (*(*best_path).path.pathtarget).exprs,
                plan_qual,
                custom_exprs,
            )
        }
    } else {
        unsafe {
            tuple_planner.plan(
                tlist,
                (*(*best_path).path.pathtarget).exprs,
                plan_qual,
                custom_exprs,
            )
        }
    };
    let encoded_filters = CustomScanFilters::<P>::encode(&filters)?;
    let custom_private = unsafe {
        CustomPrivatePlan {
            provider_id_or_name: P::NAME,
            purpose: path_private.purpose,
            relation_oid,
            planned_filter_count: filters.planned.len(),
            binding_count: filters.bindings.len(),
            provider_metadata,
            tuple_layout: &scan_tuple.layout,
            planned_filters: encoded_filters.planned,
            binding_slots: encoded_filters.bindings,
        }
        .encode()
    }?;

    let cscan = unsafe {
        pg_sys::palloc0(size_of::<pg_sys::CustomScan>()).cast::<pg_sys::CustomScan>()
    };

    unsafe {
        let scan = &mut (*cscan).scan;
        let plan = &mut scan.plan;

        plan.type_ = pg_sys::NodeTag::T_CustomScan;
        // Mirror `copy_generic_path_info` (not exposed in pg_sys).
        let path_ptr = best_path.cast::<pg_sys::Path>();
        plan.startup_cost = (*path_ptr).startup_cost;
        plan.total_cost = (*path_ptr).total_cost;
        plan.plan_rows = (*path_ptr).rows;
        plan.plan_width = (*(*path_ptr).pathtarget).width;
        plan.parallel_aware = (*path_ptr).parallel_aware;
        plan.parallel_safe = (*path_ptr).parallel_safe;
        plan.async_capable = false;
        plan.plan_node_id = 0;
        plan.targetlist = tlist;
        plan.qual = plan_qual;
        plan.lefttree = ptr::null_mut();
        plan.righttree = ptr::null_mut();
        plan.initPlan = ptr::null_mut();
        plan.extParam = ptr::null_mut();
        plan.allParam = ptr::null_mut();

        scan.scanrelid = (*rel).relid;

        (*cscan).flags = (*best_path).flags;
        (*cscan).custom_plans = custom_plans;
        (*cscan).custom_exprs = custom_exprs;
        (*cscan).custom_private = custom_private;
        (*cscan).custom_scan_tlist = scan_tuple.custom_scan_tlist;
        (*cscan).custom_relids = pg_sys::bms_make_singleton((*rel).relid as c_int);
        (*cscan).methods = method_tables_for::<P>().scan();
    }

    Ok(cscan.cast::<pg_sys::Plan>())
}

/// Classifies each final clause by whether it came from baserestrictinfo or a
/// parameterized-path join clause.
#[derive(Debug, Clone, Copy)]
struct FinalScanClauseSources {
    baserestrictinfo: *mut pg_sys::List,
}

impl FinalScanClauseSources {
    /// # Safety
    ///
    /// `rel` must be a live planner-owned node from the current
    /// `PlanCustomPath` call.
    unsafe fn new(rel: *mut pg_sys::RelOptInfo) -> Self {
        Self {
            baserestrictinfo: unsafe { (*rel).baserestrictinfo },
        }
    }

    fn source_for(self, rinfo: *mut pg_sys::RestrictInfo) -> ScanClauseSource {
        if unsafe {
            pg_sys::list_member_ptr(self.baserestrictinfo, rinfo.cast::<c_void>())
        } {
            ScanClauseSource::BaseRestriction
        } else {
            ScanClauseSource::Movable
        }
    }
}
