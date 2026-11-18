//! Planner-owned query tree and provider scan inputs before path encoding.

use lagodb_core::query_contract::ScanId;
use lagodb_query::plan::QueryPlanData;
use pgrx::pg_sys;

use super::expression::ScopedRuntimeBinding;
use super::table_scan_filter::TableScanFilter;

pub(super) struct PlannedScanInput {
    pub(super) scan: ScanId,
    pub(super) root: *mut pg_sys::PlannerInfo,
    pub(super) input_rel: *mut pg_sys::RelOptInfo,
    pub(super) range_table_index: pg_sys::Index,
    pub(super) range_table_entry: *mut pg_sys::RangeTblEntry,
    pub(super) projected_columns: Vec<pg_sys::AttrNumber>,
    pub(super) table_scan_filter: Option<TableScanFilter>,
    pub(super) estimated_rows: f64,
}

pub(super) struct PlannedQuery {
    pub(super) query: QueryPlanData,
    pub(super) runtime_exprs: Vec<ScopedRuntimeBinding>,
    pub(super) scan_target_exprs: Vec<*mut pg_sys::Expr>,
    pub(super) scans: Box<[PlannedScanInput]>,
    /// PostgreSQL's cost of projecting the path target over the scan slots,
    /// in PostgreSQL's own cost units. Zero when the engine computes every
    /// target expression.
    pub(super) postgres_projection_cost: pg_sys::QualCost,
}

impl PlannedQuery {
    /// No target expression is left to PostgreSQL's projection.
    pub(super) const NO_POSTGRES_PROJECTION: pg_sys::QualCost = pg_sys::QualCost {
        startup: 0.0,
        per_tuple: 0.0,
    };
}
