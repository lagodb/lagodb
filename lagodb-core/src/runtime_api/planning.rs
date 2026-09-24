//! Exact-build planner callback facets routed by `lagodb-base`.

use std::ffi::c_void;

use pgrx::pg_sys;

use super::CallbackErrorReport;

pub type RoutedRelationInfo = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    relation_oid: pg_sys::Oid,
    inhparent: bool,
    rel: *mut pg_sys::RelOptInfo,
    error: *mut CallbackErrorReport,
) -> u32;

pub type RoutedRelationScanPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
    error: *mut CallbackErrorReport,
) -> u32;

/// Relation CustomScan planning facet owned by one provider DSO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelationScanPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub relation_info: Option<RoutedRelationInfo>,
    pub plan_relation: Option<RoutedRelationScanPlanner>,
}

/// Prepare one rewrite-complete Query after its partitioned table RTEs are prepared.
/// The runtime owns tree traversal and invokes this for nested queries and CTEs;
/// callbacks prepare only the supplied Query and must not recurse themselves.
pub type RoutedModifyQueryPreparation = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    parse: *mut pg_sys::Query,
    error: *mut CallbackErrorReport,
) -> u32;

pub type RoutedModifyPlannerPost = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    planned: *mut pg_sys::PlannedStmt,
    error: *mut CallbackErrorReport,
) -> u32;

pub type RoutedModifyUpperPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Modify planning facet, kept distinct from relation and query planning.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub prepare_query: Option<RoutedModifyQueryPreparation>,
    pub planner_post: Option<RoutedModifyPlannerPost>,
    pub create_upper_paths: Option<RoutedModifyUpperPlanner>,
}
