//! Construction of PostgreSQL CustomPath and CustomScan nodes.

use std::ffi::{c_int, c_void};
use std::mem::size_of;
use std::ptr;

use lagodb_core::customscan::custom_exprs::PgExpressionSections;
use lagodb_query::plan::{PlanCost, SelectedQueryPlan};
use pgrx::{pg_guard, pg_sys};

use super::super::error::QueryHostError;
use super::super::methods;

pub(super) unsafe fn add_path(
    output_rel: *mut pg_sys::RelOptInfo,
    path_target: *mut pg_sys::PathTarget,
    selected_plan: *mut pg_sys::List,
    cost: PlanCost,
    rows: f64,
    parameter_info: *mut pg_sys::ParamPathInfo,
    pathkeys: *mut pg_sys::List,
) {
    let custom_path = unsafe {
        pg_sys::palloc0(size_of::<pg_sys::CustomPath>()).cast::<pg_sys::CustomPath>()
    };
    unsafe {
        let path = &mut (*custom_path).path;
        path.type_ = pg_sys::NodeTag::T_CustomPath;
        path.pathtype = pg_sys::NodeTag::T_CustomScan;
        path.parent = output_rel;
        path.pathtarget = path_target;
        path.param_info = parameter_info;
        path.parallel_aware = false;
        // This is part of the expression-safety contract: parallel-unsafe PG
        // fallback expressions execute only in the leader's serial plan.
        path.parallel_safe = false;
        path.parallel_workers = 0;
        path.rows = rows;
        path.startup_cost = cost.startup();
        path.total_cost = cost.total();
        path.pathkeys = pathkeys;

        // The encoded query fixes both its tuple layout and the corresponding
        // PostgreSQL scan expressions.  PostgreSQL must therefore place a
        // ProjectionPath above this path when it needs a different target,
        // rather than mutating the CustomPath target in place.
        (*custom_path).flags = 0;
        (*custom_path).custom_paths = ptr::null_mut();
        (*custom_path).custom_restrictinfo = ptr::null_mut();
        (*custom_path).custom_private = selected_plan;
        (*custom_path).methods = methods::tables().path();
        pg_sys::add_path(output_rel, path);
    }
}

/// Replace one PostgreSQL wrapper path whose semantics have been absorbed by
/// the query IR. Other competing paths remain untouched and `add_path` still
/// performs PostgreSQL's normal dominance tournament.
pub(super) unsafe fn replace_path(
    output_rel: *mut pg_sys::RelOptInfo,
    old_index: i32,
    selected_plan: *mut pg_sys::List,
    cost: PlanCost,
) {
    let old_path = unsafe { pg_sys::list_nth((*output_rel).pathlist, old_index) };
    let old_path_fields = unsafe { &*old_path.cast::<pg_sys::Path>() };
    let path_target = old_path_fields.pathtarget;
    let rows = old_path_fields.rows;
    let parameter_info = old_path_fields.param_info;
    let pathkeys = old_path_fields.pathkeys;
    unsafe {
        (*output_rel).pathlist =
            pg_sys::list_delete_nth_cell((*output_rel).pathlist, old_index);
        pg_sys::pfree(old_path);
        add_path(
            output_rel,
            path_target,
            selected_plan,
            cost,
            rows,
            parameter_info,
            pathkeys,
        );
    }
}

#[pg_guard]
pub(in crate::query_host) unsafe extern "C-unwind" fn plan_custom_path(
    _root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    target_list: *mut pg_sys::List,
    _clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    match unsafe { materialize_plan(best_path, target_list) } {
        Ok(plan) => plan,
        Err(error) => error.into_report().report(),
    }
}

unsafe fn materialize_plan(
    best_path: *mut pg_sys::CustomPath,
    target_list: *mut pg_sys::List,
) -> Result<*mut pg_sys::Plan, QueryHostError> {
    let selected =
        unsafe { SelectedQueryPlan::decode_path(&*(*best_path).custom_private) }
            .map_err(QueryHostError::invalid_plan)?;
    let scan_target_list =
        unsafe { build_scan_target_list(target_list, selected.scan_target_exprs())? };
    let count = unsafe { pg_sys::list_length(selected.runtime_exprs()) } as usize;
    let runtime_exprs = (0..count)
        .map(|index| {
            let expression =
                unsafe { pg_sys::list_nth(selected.runtime_exprs(), index as c_int) };
            unsafe { pg_sys::copyObjectImpl(expression) }.cast::<pg_sys::Expr>()
        })
        .collect::<Vec<_>>();
    let custom_exprs = unsafe { PgExpressionSections::encode(&runtime_exprs, &[]) };
    let custom_private = unsafe {
        SelectedQueryPlan::encode_execution(
            selected.query(),
            selected.execution_profile(),
            selected.scans(),
        )
    }
    .map_err(QueryHostError::invalid_plan)?;
    let custom_scan = unsafe {
        pg_sys::palloc0(size_of::<pg_sys::CustomScan>()).cast::<pg_sys::CustomScan>()
    };
    unsafe {
        let plan = &mut (*custom_scan).scan.plan;
        let path = &(*best_path).path;
        plan.type_ = pg_sys::NodeTag::T_CustomScan;
        plan.startup_cost = path.startup_cost;
        plan.total_cost = path.total_cost;
        plan.plan_rows = path.rows;
        plan.plan_width = (*path.pathtarget).width;
        plan.parallel_aware = false;
        plan.parallel_safe = false;
        plan.async_capable = false;
        plan.targetlist = target_list;
        plan.qual = ptr::null_mut();
        plan.lefttree = ptr::null_mut();
        plan.righttree = ptr::null_mut();
        plan.initPlan = ptr::null_mut();
        plan.extParam = ptr::null_mut();
        plan.allParam = ptr::null_mut();

        (*custom_scan).scan.scanrelid = 0;
        (*custom_scan).flags = (*best_path).flags;
        (*custom_scan).custom_plans = ptr::null_mut();
        (*custom_scan).custom_exprs = custom_exprs;
        (*custom_scan).custom_private = custom_private;
        (*custom_scan).custom_scan_tlist = scan_target_list;
        // PostgreSQL owns `custom_relids`: create_customscan_plan assigns it
        // from `best_path->path.parent->relids` after this callback returns.
        (*custom_scan).methods = methods::tables().scan();
    }
    Ok(custom_scan.cast())
}

unsafe fn build_scan_target_list(
    target_list: *mut pg_sys::List,
    scan_target_exprs: *mut pg_sys::List,
) -> Result<*mut pg_sys::List, QueryHostError> {
    let target_count = unsafe { pg_sys::list_length(target_list) };
    if target_count != unsafe { pg_sys::list_length(scan_target_exprs) } {
        return Err(QueryHostError::invalid_plan(
            "scan target expression count does not match the PostgreSQL target list",
        ));
    }
    let mut scan_target_list = ptr::null_mut();
    for index in 0..target_count {
        let target = unsafe { pg_sys::list_nth(target_list, index) }
            .cast::<pg_sys::TargetEntry>();
        let scan_target = unsafe {
            pg_sys::copyObjectImpl(target.cast::<c_void>())
                .cast::<pg_sys::TargetEntry>()
        };
        let scan_expression = unsafe { pg_sys::list_nth(scan_target_exprs, index) };
        unsafe {
            (*scan_target).expr =
                pg_sys::copyObjectImpl(scan_expression).cast::<pg_sys::Expr>();
            scan_target_list =
                pg_sys::lappend(scan_target_list, scan_target.cast::<c_void>());
        }
    }
    Ok(scan_target_list)
}
