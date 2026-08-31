//! Exact-build planner callback facets routed by `lagodb-base`.

use std::ffi::c_void;

use pgrx::pg_sys;

use super::FfiErrorRecord;

pub type RoutedRelationScanPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
    error: *mut FfiErrorRecord,
) -> u32;

/// Relation CustomScan planning facet owned by one provider DSO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelationScanPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub plan_relation: Option<RoutedRelationScanPlanner>,
}

pub type RoutedModifyPlannerPre = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    parse: *mut pg_sys::Query,
    error: *mut FfiErrorRecord,
) -> u32;

pub type RoutedModifyPlannerPost = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    planned: *mut pg_sys::PlannedStmt,
    error: *mut FfiErrorRecord,
) -> u32;

pub type RoutedModifyUpperPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
    error: *mut FfiErrorRecord,
) -> u32;

/// Modify planning facet, kept distinct from relation and query planning.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub planner_pre: Option<RoutedModifyPlannerPre>,
    pub planner_post: Option<RoutedModifyPlannerPost>,
    pub create_upper_paths: Option<RoutedModifyUpperPlanner>,
}
